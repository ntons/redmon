package remon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
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

// list element
type Elem struct {
	Id    string
	Value string
}

type Data struct {
	// sync/load
	Version int64 `msgpack:"version" bson:"version"`
	// for get/set
	Value string `msgpack:"value" bson:"value"`
	// for list
	List struct {
		Inc int64             `msgpack:"inc" bson:"inc"` // id auto increment
		Que []string          `msgpack:"que" bson:"que"` // ids by push order
		Map map[string]string `msgpack:"map" bson:"map"` // id to value
	} `msgpack:"list" bson:"list"`
}

func (d Data) String() string {
	b, _ := json.Marshal(&d)
	return string(b)
}

func (d *Data) getList() (list []Elem) {
	list = make([]Elem, 0, len(d.List.Que))
	for _, id := range d.List.Que {
		list = append(list, Elem{Id: id, Value: d.List.Map[id]})
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

// Get value from remon, if redis cache miss, load from mongo
func (x *ReMon) Get(ctx context.Context, key string) (value string, err error) {
	if value, err = x.getValueFromRedis(ctx, key); err != nil {
		if err != redis.Nil {
			return
		}
		var data Data
		if data, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		value = data.Value
	}
	return
}

// Set value to redis cache, data will be saved to mongo by sync
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

// Get list value from remon, if redis cache miss, load from mongodb
func (x *ReMon) GetList(ctx context.Context, key string) (list []Elem, err error) {
	if list, err = x.getListFromRedis(ctx, key); err != nil {
		if err != redis.Nil {
			return
		}
		var data Data
		if data, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		list = data.getList()
	}
	return
}

// Push element(s) to list
func (x *ReMon) Push(ctx context.Context, key string, values []string, opts ...PushOption) (err error) {
	var o = &pushOptions{}
	for _, opt := range opts {
		opt.apply(o)
	}
	if err = x.pushToRedis(ctx, key, values, o); err != nil {
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
		if err = x.pushToRedis(ctx, key, values, o); err != nil {
			return
		}
	}
	return
}

// Pull element(s) from list
func (x *ReMon) Pull(ctx context.Context, key string, ids []string) (n int, err error) {
	if n, err = x.pullFromRedis(ctx, key, ids); err != nil {
		if err != redis.Nil {
			return
		}
		if _, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		if n, err = x.pullFromRedis(ctx, key, ids); err != nil {
			return
		}
	}
	return
}

// Eval custom lua script on json value
// If src cannot be trusted, set lua-time-limit to a small value
func (x *ReMon) EvalVarJSON(ctx context.Context, key, src string) (ret int, value string, err error) {
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

func (x *ReMon) getDataFromRedis(ctx context.Context, key string) (data Data, err error) {
	cmd := redis.NewStringCmd("get", key)
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
		cmd = pipe.Get(key)
		pipe.ExecContext(ctx)
		if err = cmd.Err(); err != nil {
			if err == redis.Nil {
				x.stat.RedisMiss++
			} else {
				x.stat.RedisError++
			}
		} else {
			x.stat.RedisHit++
		}
	}
	if err != nil {
		return
	}
	if err = msgpack.Unmarshal(s2b(cmd.Val()), &data); err != nil {
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

func (x *ReMon) eval(ctx context.Context, lua *redis.Script, key string, args ...interface{}) (cmd *redis.Cmd) {
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
		pipe.ExecContext(ctx)
		if err = cmd.Err(); err != nil {
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

func (x *ReMon) evalVarJSON(ctx context.Context, key, src string) (ret int, value string, err error) {
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
func (x *ReMon) loadDataFromMongoToRedis(ctx context.Context, key string) (data Data, err error) {
	database, collection, _id := x.o.keyMappingStrategy.MapKey(key)
	if err = x.m.Database(database).Collection(collection).FindOne(
		ctx, bson.M{"_id": _id}).Decode(&data); err != nil {
		if err != mongo.ErrNoDocuments {
			x.stat.MongoError++
			return
		}
		err = nil
	}
	if data.List.Que == nil {
		data.List.Que = []string{}
	}
	if data.List.Map == nil {
		// keeping at least one element in map
		// since lua regards empty table as array
		data.List.Map = map[string]string{"x": ""}
	}
	b, err := msgpack.Marshal(data)
	if err != nil {
		x.stat.DataError++
		return
	}
	s, err := x.eval(ctx, luaLoadData, key,
		b2s(b), // ARGV[1]
		int64(x.o.volatileTTL/time.Millisecond), // ARGV[2]
	).Text()
	if err != nil {
		return
	}
	if err = msgpack.Unmarshal(s2b(s), &data); err != nil {
		x.stat.DataError++
		return
	}
	if data.Version == 0 {
		err = ErrNotExist
		return
	}
	return
}

func (x *ReMon) getListFromRedis(ctx context.Context, key string) (list []Elem, err error) {
	data, err := x.getDataFromRedis(ctx, key)
	if err != nil {
		return
	}
	if data.Version == 0 {
		err = ErrNotExist
		return
	}
	list = data.getList()
	return
}

func (x *ReMon) pushToRedis(ctx context.Context, key string, values []string, o *pushOptions) (err error) {
	var args = make([]interface{}, 0, len(values)+1)
	for _, value := range values {
		args = append(args, value)
	}
	args = append(args, o.capacity)
	return x.eval(ctx, luaPush, key, args...).Err()
}

func (x *ReMon) pullFromRedis(ctx context.Context, key string, ids []string) (n int, err error) {
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return x.eval(ctx, luaPull, key, args...).Int()
}
