package remon

import (
	"context"
	"fmt"
	"time"

	"github.com/ntons/redis"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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

// 邮箱
type xMailBox struct {
	Seq int64   `msgpack:"seq"` // id generater
	Que []*Mail `msgpack:"que"` // queue, ordered by id
}

// 邮件
type Mail struct {
	// auto-generated seq to identify a mail
	Id int64 `msgpack:"id"`
	// mail payload
	Val string `msgpack:"val"`
}

// 客户端
type Client struct {
	*xOptions
	// redis/mongo clients
	rdb redis.Client
	mdb *mongo.Client
}

func NewClient(rdb redis.Client, mdb *mongo.Client, opts ...Option) *Client {
	o := &xOptions{}
	for _, opt := range opts {
		opt.apply(o)
	}
	return &Client{xOptions: o, rdb: rdb, mdb: mdb}
}

// 获取数据，如果指定数据不在缓存里会自动从DB加载
func (cli *Client) Get(ctx context.Context, key string, opts ...GetOption) (rev int64, val string, err error) {
	var xopts xGetOptions
	for _, opt := range opts {
		opt.apply(&xopts)
	}
	if rev, val, err = cli.rget(ctx, key, xopts); err == redis.Nil {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		rev, val, err = cli.rget(ctx, key, xopts)
	}
	return
}

// 获取缓存数据
func (cli *Client) rget(ctx context.Context, key string, opts xGetOptions) (_ int64, _ string, err error) {
	var args []any
	if opts.addIfNotExists != nil {
		args = append(args, *opts.addIfNotExists)
	}
	s, err := cli.run(ctx, "remon_get", key, args...).Text()
	if err != nil {
		return
	}
	var data xRedisData
	if err = msgpack.Unmarshal(s2b(s), &data); err != nil {
		return
	}
	if data.Rev == 0 {
		return 0, "", ErrNotExists
	}
	return data.Rev, data.Val, nil
}

// 设置数据，如果指定数据不在缓存里会自动从DB加载
// 缓存中的脏数据由Sync异步回写到DB
func (cli *Client) Set(ctx context.Context, key, val string) (rev int64, err error) {
	if rev, err = cli.rset(ctx, key, val); err == redis.Nil {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		rev, err = cli.rset(ctx, key, val)
	}
	return
}

// 设置缓存数据
func (cli *Client) rset(ctx context.Context, key, val string) (rev int64, err error) {
	return cli.run(ctx, "remon_set", key, val).Int64()
}

// 新增数据，如果指定数据不在缓存里会自动从DB加载
// 如果指定数据已存在返回ErrAlreadyExists
func (cli *Client) Add(ctx context.Context, key, val string) (err error) {
	if err = cli.radd(ctx, key, val); err == redis.Nil {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		err = cli.radd(ctx, key, val)
	}
	return
}

// 新增缓存数据
func (cli *Client) radd(ctx context.Context, key, val string) error {
	if r, err := cli.run(ctx, "remon_add", key, val).Int64(); err != nil {
		return err
	} else if r == 0 {
		return ErrAlreadyExists
	}
	return nil
}

// 获取邮箱数据，如果指定数据不在缓存里会自动从DB加载
func (cli *Client) List(ctx context.Context, key string) (_ []*Mail, err error) {
	_, val, err := cli.Get(ctx, key)
	if err != nil {
		return
	}
	var data xMailBox
	if err = msgpack.Unmarshal(s2b(val), &data); err != nil {
		return
	}
	return data.Que, nil
}

// 添加邮件，如果指定数据不在缓存里会自动从DB加载
func (cli *Client) Push(ctx context.Context, key, val string, opts ...PushOption) (id int64, err error) {
	var xopts xPushOptions
	for _, opt := range opts {
		opt.apply(&xopts)
	}
	if id, err = cli.rpush(ctx, key, val, xopts); err == redis.Nil {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		id, err = cli.rpush(ctx, key, val, xopts)
	}
	return
}

// 添加缓存邮件
func (cli *Client) rpush(ctx context.Context, key, val string, opts xPushOptions) (id int64, err error) {
	var args = []any{val, opts.importance, opts.capacity, opts.strategy}
	if id, err = cli.run(ctx, "remon_mb_push", key, args...).Int64(); err != nil {
		return
	}
	if id == -1 {
		return 0, ErrMailBoxFull
	}
	return
}

// 删除邮件，如果指定数据不在缓存里会自动从DB加载
func (cli *Client) Pull(ctx context.Context, key string, ids ...int64) (pulled []int64, err error) {
	if len(ids) == 0 {
		return
	}
	if pulled, err = cli.rpull(ctx, key, ids...); err == redis.Nil {
		if err = cli.load(ctx, key); err != nil {
			return
		}
		pulled, err = cli.rpull(ctx, key, ids...)
	}
	return
}

// 删除缓存邮件
func (cli *Client) rpull(ctx context.Context, key string, ids ...int64) (pulled []int64, err error) {
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	r, err := cli.run(ctx, "remon_mb_pull", key, args...).Result()
	if err != nil {
		return
	}
	for _, v := range r.([]interface{}) {
		pulled = append(pulled, v.(int64))
	}
	return
}

// run script and deal errors and stats
func (cli *Client) run(ctx context.Context, cmd string, key string, args ...any) (r *redis.Cmd) {
	return luaScript.Run(ctx, cli.rdb, []string{key}, append([]any{cmd}, args...)...)
}

// Load data from database to cache
// Cache only be updated when not exists or the loaded data is newer
func (cli *Client) load(ctx context.Context, key string) (err error) {
	database, collection, _id := cli.mapKey(key)
	var data xMongoData
	if err = cli.mdb.Database(database).Collection(collection).FindOne(
		ctx, bson.M{"_id": _id}).Decode(&data); err != nil {
		if err != mongo.ErrNoDocuments {
			return
		}
		err = nil
	}
	var buf []byte
	if buf, err = msgpack.Marshal(&xRedisData{
		Rev: data.Rev,
		Val: b2s(data.Val),
	}); err != nil {
		return
	}
	if err = cli.run(ctx, "remon_load", key, b2s(buf)).Err(); err != nil {
		return
	}
	return
}

////////////////////////////////////////////////////////////////////////////////
// Syncing
////////////////////////////////////////////////////////////////////////////////
func (cli *Client) Sync(ctx context.Context) {
	var backoff *time.Timer
	for {
		for key, data, err := cli.peek(ctx); ; key, data, err = cli.next(ctx, key, data.Rev) {
			if err == nil {
				err = cli.save(ctx, key, data)
			}
			var d time.Duration
			if err == nil {
				d = cli.onSyncSave(key)
			} else if err == redis.Nil {
				d = cli.onSyncIdle()
			} else {
				d = cli.onSyncFail(err)
			}
			if d > 0 {
				if backoff == nil {
					backoff = time.NewTimer(d)
					defer backoff.Stop()
				} else {
					backoff.Reset(d)
				}
				select {
				case <-ctx.Done():
					return
				case <-backoff.C:
				}
			}
			if err != nil {
				break
			}
		}
	}
}

// peek top dirty key and data
func (cli *Client) peek(ctx context.Context) (string, xRedisData, error) {
	return cli.getSyncRes(luaScript.Run(ctx, cli.rdb, []string{}, "remon_sync"))
}

// clean dirty flag and make key volatile, then peek the next
func (cli *Client) next(ctx context.Context, key string, rev int64) (string, xRedisData, error) {
	return cli.getSyncRes(luaScript.Run(ctx, cli.rdb, []string{key}, "remon_sync", rev))
}

func (*Client) getSyncRes(r *redis.Cmd) (key string, data xRedisData, err error) {
	var v interface{}
	if v, err = r.Result(); err != nil {
		return
	} else if a, ok := v.([]interface{}); !ok || len(a) != 2 {
		panic(fmt.Errorf("unexpected return type: %T", r))
	} else if err = msgpack.Unmarshal(s2b(a[1].(string)), &data); err != nil {
		return
	} else {
		return a[0].(string), data, nil
	}
}

func (cli *Client) save(ctx context.Context, key string, data xRedisData) (err error) {
	database, collection, _id := cli.mapKey(key)
	_, err = cli.mdb.Database(database).Collection(collection).UpdateOne(
		ctx,
		bson.M{"_id": _id},
		bson.M{"$set": &xMongoData{
			Rev: data.Rev,
			Val: s2b(data.Val),
		}},
		options.Update().SetUpsert(true),
	)
	return
}
