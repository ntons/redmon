package remon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/ntons/tongo/tunsafe"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type RedisClient interface {
	scripter
	Pipeline() redis.Pipeliner
	ProcessContext(context.Context, redis.Cmder) error
}

var (
	// exported errors
	ErrNotExist = errors.New("remon: not exist")
)

func isBusyError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "BUSY")
}

type Mail struct {
	Id    string
	Value string
}

type MailBytes struct {
	Id    string
	Value []byte
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

type DB = ReMon

type ReMon struct {
	// options
	o *options
	// redis client
	r RedisClient
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
	return &ReMon{o: o, r: r, m: m}
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
func (x *ReMon) GetBytes(
	ctx context.Context, key string) (value []byte, err error) {
	s, err := x.Get(ctx, key)
	if err == nil {
		value = tunsafe.StringToBytes(s)
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
func (x *ReMon) SetBytes(
	ctx context.Context, key string, value []byte) (err error) {
	return x.Set(ctx, key, tunsafe.BytesToString(value))
}

// eval custom lua script on json value
// if src cannot be trusted, set lua-time-limit to a small value
func (x *ReMon) EvalVarJSON(
	ctx context.Context, key, src string) (ret int, value string, err error) {
	if ret, value, err = x.evalVarJSON(ctx, key, src); err != nil {
		if err != redis.Nil {
			return
		}
		if _, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			if err != ErrNotExist {
				return
			}
		}
		if ret, value, err = x.evalVarJSON(ctx, key, src); err != nil {
			return
		}
	}
	return
}

// redis access
func (x *ReMon) getDataFromRedis(
	ctx context.Context, key string) (data _Data, err error) {
	cmd := redis.NewStringCmd("get", key)
	if err = x.r.ProcessContext(ctx, cmd); err != nil {
		if err == redis.Nil {
			x.stat.RedisMiss++
		} else {
			x.stat.RedisError++
		}
		return
	} else {
		x.stat.RedisHit++
	}
	for isBusyError(err) {
		pipe := x.r.Pipeline()
		pipe.ScriptKill()
		cmd = pipe.Get(key)
		if _, err = pipe.ExecContext(ctx); err != nil {
			if err == redis.Nil {
				x.stat.RedisMiss++
			} else {
				x.stat.RedisError++
			}
		} else {
			x.stat.RedisHit++
		}
	}
	if err = msgpack.Unmarshal(tunsafe.StringToBytes(cmd.Val()), &data); err != nil {
		return
	}
	return
}

func (x *ReMon) getValueFromRedis(
	ctx context.Context, key string) (value string, err error) {
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
	ctx context.Context, lua *redis.Script, key string, args ...interface{}) (
	cmd *redis.Cmd) {
	cmdArgs := make([]interface{}, 4, 4+len(args))
	cmdArgs[0] = "evalsha"
	cmdArgs[1] = lua.Hash()
	cmdArgs[2] = 1
	cmdArgs[3] = key
	cmdArgs = append(cmdArgs, args...)
	cmd = redis.NewCmd(cmdArgs...)

	var err error
	if err = x.r.ProcessContext(ctx, cmd); err != nil {
		if err == redis.Nil {
			x.stat.RedisMiss++
		} else {
			x.stat.RedisError++
		}
	} else {
		x.stat.RedisHit++
	}

	for isBusyError(err) {
		pipe := x.r.Pipeline()
		pipe.ScriptKill()
		cmd = pipe.EvalSha(lua.Hash(), []string{key}, args...)
		if _, err = pipe.ExecContext(ctx); err != nil {
			if err == redis.Nil {
				x.stat.RedisMiss++
			} else {
				x.stat.RedisError++
			}
		} else {
			x.stat.RedisHit++
		}
	}
	return
}

func (x *ReMon) addValueToRedis(
	ctx context.Context, key, value string) (err error) {
	return
}

func (x *ReMon) setValueToRedis(
	ctx context.Context, key, value string) (err error) {
	return x.eval(ctx, luaSetValue, key, value).Err()
}

func (x *ReMon) evalVarJSON(
	ctx context.Context, key, src string) (ret int, value string, err error) {
	r, err := x.eval(ctx, luaEvalVarJSON, key, src).Result()
	if err != nil {
		return
	}
	v, ok := r.([]interface{})
	if !ok {
		err = fmt.Errorf("bad script return type: %T", r)
		return
	}
	if i64, ok := v[0].(int64); !ok {
		err = fmt.Errorf("bad script return type: %T", v[0])
		return
	} else {
		ret = int(i64)
	}
	if value, ok = v[1].(string); !ok {
		err = fmt.Errorf("bad script return type: %T", v[1])
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
		err = nil
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
	s, err := x.eval(ctx, luaLoadData, key,
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
