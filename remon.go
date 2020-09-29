package remon

import (
	"context"
	"fmt"

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

type data struct {
	Rev int64  `msgpack:"rev" bson:"rev" json:"rev"`
	Val string `msgpack:"val" bson:"val" json:"val"`
}

type Client = ReMon

type ReMon struct {
	o *options
	r RedisClient
	m *mongo.Client
	s Stat
}

func New(r RedisClient, m *mongo.Client, opts ...Option) (x *ReMon) {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	return &ReMon{o: o, r: r, m: m}
}

func (x *ReMon) Stat() *Stat { return &x.s }

// Get value from remon, if cache miss, load from database
func (x *ReMon) Get(
	ctx context.Context, key string, opts ...GetOption) (val string, err error) {
	if val, err = x.get(ctx, key, opts...); isCacheMiss(err) {
		if err = x.load(ctx, key); err != nil {
			return
		}
		val, err = x.get(ctx, key, opts...)
	}
	return
}
func (x *ReMon) GetBytes(
	ctx context.Context, key string, opts ...GetOption) (_ []byte, err error) {
	s, err := x.Get(ctx, key, opts...)
	if err != nil {
		return
	}
	return s2b(s), nil
}

// Set value to redis cache, data will be saved to mongo by sync
func (x *ReMon) Set(ctx context.Context, key, val string) (err error) {
	if err = x.set(ctx, key, val); isCacheMiss(err) {
		if err = x.load(ctx, key); err != nil {
			return
		}
		err = x.set(ctx, key, val)
	}
	return
}
func (x *ReMon) SetBytes(
	ctx context.Context, key string, val []byte) (err error) {
	return x.Set(ctx, key, b2s(val))
}

// Set value to remon if key not exists
func (x *ReMon) Add(ctx context.Context, key, val string) (err error) {
	if err = x.add(ctx, key, val); isCacheMiss(err) {
		if err = x.load(ctx, key); err != nil {
			return
		}
		err = x.add(ctx, key, val)
	}
	return
}
func (x *ReMon) AddBytes(
	ctx context.Context, key string, val []byte) (err error) {
	return x.Add(ctx, key, b2s(val))
}

// get data from cache
var luaGet = newScript(`local b=redis.call("GET", KEYS[1]) if not b then error("CACHE_MISS") end local d=cmsgpack.unpack(b) if d.rev==0 and ARGV[1] then d.rev,d.val=d.rev+1,ARGV[1] b=cmsgpack.pack(d) redis.call("SET",KEYS[1],b) if redis.call("SADD",":DIRTYSET",KEYS[1])>0 then redis.call("LPUSH",":DIRTYQUE",KEYS[1]) end end return b
`)

func (x *ReMon) get(
	ctx context.Context, key string, opts ...GetOption) (val string, err error) {
	var o getOptions
	for _, opt := range opts {
		opt.apply(&o)
	}
	var args []interface{}
	if o.addIfNotFound != nil {
		args = append(args, *o.addIfNotFound)
	}
	s, err := x.runScript(ctx, luaGet, key, args...).Text()
	if err != nil {
		return
	}
	var dat data
	if err = msgpack.Unmarshal(s2b(s), &dat); err != nil {
		x.o.log.Errorf("failed to unmarshal data: %v", err)
		x.s.dataError++
		return
	}
	if dat.Rev == 0 {
		return "", ErrNotFound
	}
	return dat.Val, nil
}

// update value to cache, return revision after updating
var luaSet = newScript(`local b=redis.call("GET",KEYS[1]) if not b then error("CACHE_MISS") end local d=cmsgpack.unpack(b) d.rev,d.val=d.rev+1,ARGV[1] redis.call("SET",KEYS[1],cmsgpack.pack(d)) if redis.call("SADD",":DIRTYSET",KEYS[1])>0 then redis.call("LPUSH",":DIRTYQUE",KEYS[1]) end return d.rev`)

func (x *ReMon) set(ctx context.Context, key, val string) error {
	return x.runScript(ctx, luaSet, key, val).Err()
}

var luaAdd = newScript(`local b=redis.call("GET",KEYS[1]) if not b then error("CACHE_MISS") end local d=cmsgpack.unpack(b) if d.rev~=0 then return 0 end d.rev,d.val=d.rev+1,ARGV[1] redis.call("SET",KEYS[1],cmsgpack.pack(d)) if redis.call("SADD",":DIRTYSET",KEYS[1])>0 then redis.call("LPUSH",":DIRTYQUE",KEYS[1]) end return 1`)

func (x *ReMon) add(ctx context.Context, key, val string) error {
	if r, err := x.runScript(ctx, luaAdd, key, val).Int(); err != nil {
		return err
	} else if r == 0 {
		return ErrAlreadyExists
	}
	return nil
}

// eval lua script on VALUE
func NewEvalScript(src string, opts ...ScriptOption) *Script {
	tmpl := `local b=redis.call("GET", KEYS[1]) if not b then error("CACHE_MISS") end local d=cmsgpack.unpack(b) local f=assert(loadstring([[%s]])) local e={} setmetatable(e,{__index=_G}) e.VALUE=d.val setfenv(f,e) local r=f() if d.val~=e.VALUE then d.rev,d.val=d.rev+1,e.VALUE redis.call("SET",KEYS[1],cmsgpack.pack(d)) if redis.call("SADD",":DIRTYSET",KEYS[1])>0 then redis.call("LPUSH",":DIRTYQUE",KEYS[1]) end end return r`
	return newScript(fmt.Sprintf(tmpl, src), opts...)
}

// Eval execute a script on value, modified value will be saved automatically
// It's dangerous to eval untrusted script, so this method is only used to
// extend remon abilities
func (x *ReMon) Eval(
	ctx context.Context, script *Script, key string, args ...interface{}) (
	cmd *redis.Cmd) {
	if cmd = x.runScript(ctx, script, key, args...); isCacheMiss(cmd.Err()) {
		if err := x.load(ctx, key); err != nil {
			return
		}
		cmd = x.runScript(ctx, script, key, args...)
	}
	return
}

// load data from database to cache
// if loaded revision was accepted return "OK", else return latest data
var luaLoad = newScript(`local b=redis.call("GET",KEYS[1]) if not b or cmsgpack.unpack(b).rev<cmsgpack.unpack(ARGV[1]).rev then redis.call("SET",KEYS[1],ARGV[1],"EX",86400) end return 0`)

func (x *ReMon) load(ctx context.Context, key string) (err error) {
	database, collection, _id := x.o.keyMappingStrategy.MapKey(key)
	var dat data
	if err = x.m.Database(database).Collection(collection).FindOne(
		ctx, bson.M{"_id": _id}).Decode(&dat); err != nil {
		if err != mongo.ErrNoDocuments {
			x.o.log.Errorf("failed to load data from mongo: %v", err)
			x.s.mongoError++
			return
		}
		err = nil
	}
	var b []byte
	if b, err = msgpack.Marshal(&dat); err != nil {
		x.o.log.Errorf("failed to marshal data: %v", err)
		x.s.dataError++
		return
	}
	if err = x.runScript(ctx, luaLoad, key, b2s(b)).Err(); err != nil {
		return
	}
	return
}

// run script
func (x *ReMon) runScript(
	ctx context.Context, script *Script, key string, args ...interface{}) (
	cmd *redis.Cmd) {
	cmd = script.Run(ctx, x.r, []string{key}, args...)
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			cmd.SetErr(nil) // not regard redis.Nil as error
		} else if isCacheMiss(err) {
			cmd.SetErr(errCacheMiss)
		}
	}
	if err := cmd.Err(); err != nil {
		if isCacheMiss(err) {
			x.s.cacheMiss++
		} else {
			x.s.redisError++
		}
		return
	}
	x.s.cacheHit++
	return
}
