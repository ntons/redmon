package remon

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	xDirtySet = "$DIRTYSET$"
	xDirtyQue = "$DIRTYQUE$"
)

var (
	luaGet = newScript(`
local b=redis.call("GET", KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
if d.rev==0 and ARGV[1] then
  d.rev,d.val=d.rev+1,ARGV[1]
  b=cmsgpack.pack(d)
  redis.call("SET",KEYS[1],b)
  if redis.call("SADD","` + xDirtySet + `",KEYS[1])>0 then
    redis.call("LPUSH","` + xDirtyQue + `",KEYS[1])
  end
end
return b
`)

	luaSet = newScript(`
local b=redis.call("GET",KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
d.rev,d.val=d.rev+1,ARGV[1]
redis.call("SET",KEYS[1],cmsgpack.pack(d))
if redis.call("SADD","` + xDirtySet + `",KEYS[1])>0 then
  redis.call("LPUSH","` + xDirtyQue + `",KEYS[1])
end
return d.rev
`)

	luaAdd = newScript(`
local b=redis.call("GET",KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
if d.rev~=0 then return 0 end
d.rev,d.val=d.rev+1,ARGV[1]
redis.call("SET",KEYS[1],cmsgpack.pack(d))
if redis.call("SADD","` + xDirtySet + `",KEYS[1])>0 then
  redis.call("LPUSH","` + xDirtyQue + `",KEYS[1])
end
return 1
`)

	luaLoad = newScript(`
local b=redis.call("GET",KEYS[1])
if not b or cmsgpack.unpack(b).rev<cmsgpack.unpack(ARGV[1]).rev then
  redis.call("SET",KEYS[1],ARGV[1],"EX",86400)
end
return 0
`)
)

type Client interface {
	// get stat info
	Stat() *Stat

	// Get value from remon
	// If cache miss, load from database
	Get(ctx context.Context, key string, opts ...GetOption) (rev int64, val string, err error)

	// Set value to remon
	// Value will be saved to cache and synced to database later automatically
	Set(ctx context.Context, key, val string) (rev int64, err error)

	// Add value to remon
	// If key already exists, return ErrAlreadyExists
	// If add success, the rev must be 1
	Add(ctx context.Context, key, val string) (err error)

	// Eval execute a script on value, modified value will be saved automatically
	// It's dangerous to eval untrusted script
	// This method is only used to extend remon abilities
	eval(ctx context.Context, script *xScript, key string, args ...interface{}) (cmd *redis.Cmd)
}

// REDIS存储数据对象(cmsgpack不接受bin数据类型，只能用string)
type xRedisData struct {
	// 数据版本号，每次修改都会递增。
	// lua的number是由double实现，一般情况下有52bits尾数位，因此最大支持
	// 整数大约为4.5*10^15，我不觉得有哪些数据能达到千万亿级别的修改次数。
	Rev int64 `msgpack:"rev" bson:"rev" json:"rev"`
	// 数据有效载荷
	Val string `msgpack:"val" bson:"val" json:"val"`
}

// MONGO存储数据对象
type xMongoData struct {
	Rev int64  `msgpack:"rev" bson:"rev" json:"rev"`
	Val []byte `msgpack:"val" bson:"val" json:"val"`
}

var _ Client = (*xClient)(nil)

type xClient struct {
	opts *xOptions
	rdb  RedisClient
	mdb  *mongo.Client
	stat Stat
}

func NewClient(rdb RedisClient, mdb *mongo.Client, opts ...Option) Client {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	return &xClient{opts: o, rdb: rdb, mdb: mdb}
}
func New(rdb RedisClient, mdb *mongo.Client, opts ...Option) Client {
	return NewClient(rdb, mdb, opts...)
}

func (cli *xClient) Stat() *Stat { return &cli.stat }

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

func (cli *xClient) Add(ctx context.Context, key, val string) (err error) {
	if err = cli.add(ctx, key, val); isCacheMiss(err) {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		err = cli.add(ctx, key, val)
	}
	return
}

func (cli *xClient) eval(
	ctx context.Context, script *xScript, key string, args ...interface{}) (
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
	ctx context.Context, script *xScript, key string, args ...interface{}) (
	cmd *redis.Cmd) {
	cmd = script.Run(ctx, cli.rdb, []string{key}, args...)
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			cmd.SetErr(nil) // not regard redis.Nil as error
		} else if isCacheMiss(err) {
			cmd.SetErr(errCacheMiss)
		}
	}
	if err := cmd.Err(); err != nil {
		if isCacheMiss(err) {
			cli.stat.incrCacheMiss()
		} else {
			cli.stat.incrRedisError()
		}
	} else {
		cli.stat.incrCacheHit()
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
	var data xRedisData
	if err = msgpack.Unmarshal(fastStringToBytes(s), &data); err != nil {
		cli.opts.log.Errorf("failed to unmarshal data: %v", err)
		cli.stat.incrDataError()
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
	database, collection, _id := cli.opts.keyMappingStrategy.MapKey(key)
	var data xMongoData
	if err = cli.mdb.Database(database).Collection(collection).FindOne(
		ctx, bson.M{"_id": _id}).Decode(&data); err != nil {
		if err != mongo.ErrNoDocuments {
			cli.opts.log.Errorf("failed to load data from mongo: %v", err)
			cli.stat.incrMongoError()
			return
		}
		err = nil
	}
	var buf []byte
	if buf, err = msgpack.Marshal(&xRedisData{
		Rev: data.Rev,
		Val: fastBytesToString(data.Val),
	}); err != nil {
		cli.opts.log.Errorf("failed to marshal data: %v", err)
		cli.stat.incrDataError()
		return
	}
	if err = cli.runScript(
		ctx, luaLoad, key, fastBytesToString(buf)).Err(); err != nil {
		return
	}
	return
}
