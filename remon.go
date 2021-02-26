package remon

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// redis client interface
// maybe *redis.Client, *redis.ClusterClient etc.
type RedisClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

type Client interface {
	Stat() Stat
	Get(ctx context.Context, key string, opts ...GetOption) (rev int64, val string, err error)
	GetBytes(ctx context.Context, key string, opts ...GetOption) (int64, []byte, error)
	Set(ctx context.Context, key, val string) (rev int64, err error)
	SetBytes(ctx context.Context, key string, val []byte) (rev int64, err error)
	Add(ctx context.Context, key, val string) (err error)
	AddBytes(ctx context.Context, key string, val []byte) (err error)
	Eval(ctx context.Context, script *Script, key string, args ...interface{}) (cmd *redis.Cmd)
}

// CACHE/DB 存储结构
type xData struct {
	// 数据版本号，每次修改都会递增。
	// lua的number是由double实现，一般情况下有52bits尾数位，因此最大支持
	// 整数大约为4.5*10^15，我不觉得有哪些数据能达到千万亿级别的修改次数。
	Rev int64 `msgpack:"rev" bson:"rev" json:"rev"`
	// 数据有效载荷
	Val string `msgpack:"val" bson:"val" json:"val"`
}

var _ Client = (*xClient)(nil)

type xClient struct {
	o *xOptions
	r RedisClient
	m *mongo.Client
	s Stat
}

func NewClient(r RedisClient, m *mongo.Client, opts ...Option) Client {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	return &xClient{o: o, r: r, m: m}
}
func New(r RedisClient, m *mongo.Client, opts ...Option) Client {
	return NewClient(r, m, opts...)
}

// get stat info
func (cli *xClient) Stat() Stat { return cli.s }

// Get value from remon
// If cache miss, load from database
func (cli *xClient) Get(
	ctx context.Context, key string, opts ...GetOption) (
	rev int64, val string, err error) {
	if rev, val, err = cli.get(ctx, key, opts...); isCacheMiss(err) {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		rev, val, err = cli.get(ctx, key, opts...)
	}
	return
}
func (cli *xClient) GetBytes(
	ctx context.Context, key string, opts ...GetOption) (int64, []byte, error) {
	if rev, s, err := cli.Get(ctx, key, opts...); err != nil {
		return 0, nil, err
	} else {
		return rev, s2b(s), nil
	}
}

// Set value to remon
// Value will be saved to cache and synced to database later automatically
func (cli *xClient) Set(
	ctx context.Context, key, val string) (rev int64, err error) {
	if rev, err = cli.set(ctx, key, val); isCacheMiss(err) {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		rev, err = cli.set(ctx, key, val)
	}
	return
}
func (cli *xClient) SetBytes(
	ctx context.Context, key string, val []byte) (rev int64, err error) {
	return cli.Set(ctx, key, b2s(val))
}

// Add value to remon
// If key already exists, return ErrAlreadyExists
// If add success, the rev must be 1
func (cli *xClient) Add(ctx context.Context, key, val string) (err error) {
	if err = cli.add(ctx, key, val); isCacheMiss(err) {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		err = cli.add(ctx, key, val)
	}
	return
}
func (cli *xClient) AddBytes(
	ctx context.Context, key string, val []byte) (err error) {
	return cli.Add(ctx, key, b2s(val))
}

// Eval execute a script on value, modified value will be saved automatically
// It's dangerous to eval untrusted script, so this method is only used to
// extend remon abilities
func (cli *xClient) Eval(
	ctx context.Context, script *Script, key string, args ...interface{}) (
	cmd *redis.Cmd) {
	if cmd = cli.runScript(ctx, script, key, args...); isCacheMiss(cmd.Err()) {
		if err := cli.load(ctx, key); err != nil {
			return
		}
		cmd = cli.runScript(ctx, script, key, args...)
	}
	return
}

// run script and deal errors and stats
func (cli *xClient) runScript(
	ctx context.Context, script *Script, key string, args ...interface{}) (
	cmd *redis.Cmd) {
	cmd = script.Run(ctx, cli.r, []string{key}, args...)
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			cmd.SetErr(nil) // not regard redis.Nil as error
		} else if isCacheMiss(err) {
			cmd.SetErr(errCacheMiss)
		}
	}
	if err := cmd.Err(); err != nil {
		if isCacheMiss(err) {
			cli.s.cacheMiss++
		} else {
			cli.s.redisError++
		}
	} else {
		cli.s.cacheHit++
	}
	return
}

// Get data from cache
func (cli *xClient) get(
	ctx context.Context, key string, opts ...GetOption) (
	_ int64, _ string, err error) {
	var o xGetOptions
	for _, opt := range opts {
		opt.apply(&o)
	}
	var args []interface{}
	if o.addIfNotFound != nil {
		args = append(args, *o.addIfNotFound)
	}
	s, err := cli.runScript(ctx, luaGet, key, args...).Text()
	if err != nil {
		return
	}
	var data xData
	if err = msgpack.Unmarshal(s2b(s), &data); err != nil {
		cli.o.log.Errorf("failed to unmarshal data: %v", err)
		cli.s.dataError++
		return
	}
	if data.Rev == 0 {
		return 0, "", ErrNotFound
	}
	return data.Rev, data.Val, nil
}

// Set value to cache, return revision after updating
func (cli *xClient) set(
	ctx context.Context, key, val string) (rev int64, err error) {
	return cli.runScript(ctx, luaSet, key, val).Int64()
}

// Add value to cache
func (cli *xClient) add(ctx context.Context, key, val string) error {
	if r, err := cli.runScript(ctx, luaAdd, key, val).Int64(); err != nil {
		return err
	} else if r == 0 {
		return ErrAlreadyExists
	}
	return nil
}

// Load data from database to cache
// Cache only be updated when not exists or the loaded data is newer
func (cli *xClient) load(ctx context.Context, key string) (err error) {
	database, collection, _id := cli.o.keyMappingStrategy.MapKey(key)
	var data xData
	if err = cli.m.Database(database).Collection(collection).FindOne(
		ctx, bson.M{"_id": _id}).Decode(&data); err != nil {
		if err != mongo.ErrNoDocuments {
			cli.o.log.Errorf("failed to load data from mongo: %v", err)
			cli.s.mongoError++
			return
		}
		err = nil
	}
	var b []byte
	if b, err = msgpack.Marshal(&data); err != nil {
		cli.o.log.Errorf("failed to marshal data: %v", err)
		cli.s.dataError++
		return
	}
	if err = cli.runScript(ctx, luaLoad, key, b2s(b)).Err(); err != nil {
		return
	}
	return
}
