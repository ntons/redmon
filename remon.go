package remon

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/ntons/tongo/tunsafe"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	// exported errors
	ErrNotExist = errors.New("remon: not exist")
)

type Mail struct {
	Id    string
	Value string
}

type _Data struct {
	// sync/load
	Version int64 `msgpack:"version" bson:"version"`
	// for get/set
	Value string `msgpack:"value" bson:"value"`
	// for mail-box
	Mailbox struct {
		Inc  int64             `msgpack:"inc" bson:"inc"`   // id auto increment
		Que  []string          `msgpack:"que" bson:"que"`   // ids by push order
		Dict map[string]string `msgpack:"dict" bson:"dict"` // id to value
	} `msgpack:"mailbox" bson:"mailbox"`
}

func (d _Data) String() string {
	b, _ := json.Marshal(&d)
	return string(b)
}

func (d *_Data) getMailList() (list []Mail) {
	list = make([]Mail, 0, len(d.Mailbox.Que))
	for _, id := range d.Mailbox.Que {
		list = append(list, Mail{Id: id, Value: d.Mailbox.Dict[id]})
	}
	return
}

type ReMon struct {
	// options
	o *options
	// redis client
	r RedisClientWithContext
	// mongo client
	m *mongo.Client
	// stat
	stat Stat
}

func New(r RedisClient, m *mongo.Client, opts ...Option) (x *ReMon) {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	return &ReMon{
		o: o,
		r: NewRedisClientWithContext(r),
		m: m,
	}
}

func (x *ReMon) Stat() *Stat { return &x.stat }

func (x *ReMon) Get(ctx context.Context, key string) (value string, err error) {
	if value, err = x.getValueFromRedis(ctx, key); err != nil {
		if err != redis.Nil {
			return
		}
		var data _Data
		if data, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		value = data.Value
	}
	return
}

func (x *ReMon) Set(ctx context.Context, key, value string) (err error) {
	if err = x.setValueToRedis(ctx, key, value); err != nil {
		if err != redis.Nil {
			return
		}
		if _, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			if err != ErrNotExist {
				return
			}
		}
		if err = x.setValueToRedis(ctx, key, value); err != nil {
			return
		}
	}
	return
}

func (x *ReMon) ListMail(
	ctx context.Context, key string) (list []Mail, err error) {
	if list, err = x.getMailListFromRedis(ctx, key); err != nil {
		if err != redis.Nil {
			return
		}
		var data _Data
		if data, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		list = data.getMailList()
	}
	return
}

func (x *ReMon) PushMail(
	ctx context.Context, key, value string, opts ...PushOption) (err error) {
	var o = &pushOptions{}
	for _, opt := range opts {
		opt.apply(o)
	}
	if err = x.pushMailToRedis(ctx, key, value, o); err != nil {
		if err != redis.Nil {
			return
		}
		if _, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			if err != ErrNotExist {
				return
			}
			if !o.addOnNotExist {
				return
			}
		}
		if err = x.pushMailToRedis(ctx, key, value, o); err != nil {
			return
		}
	}
	return
}

func (x *ReMon) PullMail(ctx context.Context, key string, ids ...string) (n int, err error) {
	if n, err = x.pullMailFromRedis(ctx, key, ids); err != nil {
		if err != redis.Nil {
			return
		}
		if _, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		if n, err = x.pullMailFromRedis(ctx, key, ids); err != nil {
			return
		}
	}
	return
}

// redis access
func (x *ReMon) getDataFromRedis(ctx context.Context, key string) (data _Data, err error) {
	s, err := x.r.WithContext(ctx).Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			x.stat.RedisMiss++
		} else {
			x.stat.RedisError++
		}
		return
	} else {
		x.stat.RedisHit++
	}
	if err = msgpack.Unmarshal(tunsafe.StringToBytes(s), &data); err != nil {
		return
	}
	return
}

func (x *ReMon) getValueFromRedis(ctx context.Context, key string) (value string, err error) {
	data, err := x.getDataFromRedis(ctx, key)
	if err != nil {
		return
	}
	if data.Version == 0 {
		err = ErrNotExist
		return
	}
	value = data.Value
	return
}

func (x *ReMon) eval(
	lua *redis.Script, ctx context.Context, key string, args ...interface{}) (
	r *redis.Cmd) {
	r = lua.EvalSha(x.r.WithContext(ctx), []string{key}, args...)
	if _, err := r.Result(); err != nil {
		if err == redis.Nil {
			x.stat.RedisMiss++
		} else {
			x.stat.RedisError++
		}
	} else {
		x.stat.RedisHit++
	}
	return
}

func (x *ReMon) addValueToRedis(
	ctx context.Context, key, value string) (err error) {
	return
}

func (x *ReMon) setValueToRedis(
	ctx context.Context, key, value string) (err error) {
	if _, err = x.eval(luaSetValue, ctx, key, value).Result(); err != nil {
		return
	}
	return
}

func (x *ReMon) getMailListFromRedis(
	ctx context.Context, key string) (list []Mail, err error) {
	data, err := x.getDataFromRedis(ctx, key)
	if err != nil {
		return
	}
	if data.Version == 0 {
		err = ErrNotExist
		return
	}
	list = data.getMailList()
	return
}
func (x *ReMon) pushMailToRedis(
	ctx context.Context, key, value string, o *pushOptions) (err error) {
	if _, err = x.eval(
		luaPushMail, ctx, key, value, o.capacity).Result(); err != nil {
		return
	}
	return
}
func (x *ReMon) pullMailFromRedis(
	ctx context.Context, key string, ids []string) (n int, err error) {
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	if n, err = x.eval(luaPullMail, ctx, key, args...).Int(); err != nil {
		return
	}
	return
}

// load data from mongo to redis, including not exist status
func (x *ReMon) loadDataFromMongoToRedis(
	ctx context.Context, key string) (data _Data, err error) {
	database, collection, _id := x.o.keyMappingStrategy.MapKey(key)
	if err = x.m.Database(database).Collection(collection).FindOne(
		ctx, bson.M{"_id": _id}).Decode(&data); err != nil {
		if err != mongo.ErrNoDocuments {
			x.stat.MongoError++
			return
		}
	}
	if data.Mailbox.Que == nil {
		data.Mailbox.Que = []string{}
	}
	if data.Mailbox.Dict == nil {
		// keeping at least one element in dict
		// because lua regards empty table as array
		data.Mailbox.Dict = map[string]string{"x": ""}
	}
	b, err := msgpack.Marshal(data)
	if err != nil {
		x.stat.DataError++
		return
	}
	s, err := x.eval(luaLoadData, ctx, key,
		tunsafe.BytesToString(b),                // ARGV[1]
		int64(x.o.volatileTTL/time.Millisecond), // ARGV[2]
	).Text()
	if err != nil {
		return
	}
	if err = msgpack.Unmarshal(tunsafe.StringToBytes(s), &data); err != nil {
		x.stat.DataError++
		return
	}
	if data.Version == 0 {
		err = ErrNotExist
		return
	}
	return
}
