package remon

import (
	"context"
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

type _Data struct {
	Version int64  `msgpack:"version" bson:"version"`
	Value   string `msgpack:"value" bson:"value"`
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
	applyOptions(o, opts)
	return &ReMon{
		o: o,
		r: NewRedisClientWithContext(r),
		m: m,
	}
}

func (x *ReMon) Stat() Stat { return x.stat }

func (x *ReMon) Get(ctx context.Context, key string, opts ...GetOption) (value string, err error) {
	o := getOptions{}
	applyGetOptions(&o, opts)

	if value, err = x.getValueFromRedis(ctx, key); err != nil {
		if err != redis.Nil {
			x.stat.RedisError++
			return
		}
		x.stat.RedisMiss++

		if value, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			if err == ErrNotExist && !o.addOnNotExist {
				return
			}
			if err = x.setValueToRedis(
				ctx, key, o.addOnNotExistValue); err != nil {
				return
			}
			value = o.addOnNotExistValue
		}
	} else {
		x.stat.RedisHit++
	}
	return
}

func (x *ReMon) Set(ctx context.Context, key, value string) (err error) {
	if err = x.setValueToRedis(ctx, key, value); err != nil {
		return
	}
	return
}

// redis access
func (x *ReMon) getDataFromRedis(ctx context.Context, key string) (data _Data, err error) {
	b, err := x.r.WithContext(ctx).Get(key).Result()
	if err != nil {
		return
	}
	if err = msgpack.Unmarshal(tunsafe.StringToBytes(b), &data); err != nil {
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
	} else {
		value = data.Value
	}
	return
}

func (x *ReMon) setValueToRedis(ctx context.Context, key, value string) (err error) {
	if _, err = luaSetValue.EvalSha(
		x.r.WithContext(ctx),
		[]string{key}, // KEYS[1]
		value,         // ARGV[1]
	).Result(); err != nil && err != redis.Nil {
		return
	}
	return nil
}

// mongo access
func (x *ReMon) getDataFromMongo(ctx context.Context, key string) (data _Data, err error) {
	database, collection, _id := x.o.keyMappingStrategy.MapKey(key)
	if err = x.m.Database(database).Collection(collection).FindOne(
		ctx, bson.M{"_id": _id}).Decode(&data); err != nil {
		return
	}
	return
}

// load data from mongo to redis, including not exist status
func (x *ReMon) loadDataFromMongoToRedis(ctx context.Context, key string) (value string, err error) {
	data, err := x.getDataFromMongo(ctx, key)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return
		}
		err = nil
	}
	b, err := msgpack.Marshal(data)
	if err != nil {
		return
	}
	s, err := luaLoadData.EvalSha(
		x.r.WithContext(ctx),
		[]string{key},                           // KEYS[1]
		tunsafe.BytesToString(b),                // ARGV[1]
		int64(x.o.volatileTTL/time.Millisecond), // ARGV[2]
	).Text()
	if err != nil {
		return
	}
	if err = msgpack.Unmarshal(tunsafe.StringToBytes(s), &data); err != nil {
		return
	}
	if data.Version == 0 {
		err = ErrNotExist
	} else {
		value = data.Value
	}
	return
}
