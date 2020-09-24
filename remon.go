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
	Get(ctx context.Context, key string) *redis.StringCmd
}

type data struct {
	Rev int64  `msgpack:"rev" bson:"rev" json:"rev"`
	Val string `msgpack:"val" bson:"val" json:"val"`
}

type Database = ReMon

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
func (x *ReMon) Get(ctx context.Context, key string) (val string, err error) {
	dat, err := x.get(ctx, key)
	if isCacheMiss(err) {
		dat, err = x.load(ctx, key)
	}
	if err == nil && dat.Rev < 1 {
		return "", ErrNotFound
	}
	return dat.Val, nil
}

// Set value to redis cache, data will be saved to mongo by sync
func (x *ReMon) Set(ctx context.Context, key, val string) (err error) {
	if err = x.update(ctx, key, val); isCacheMiss(err) {
		if _, err = x.load(ctx, key); err != nil {
			return
		}
		err = x.update(ctx, key, val)
	}
	return
}

// get data from cache
func (x *ReMon) get(ctx context.Context, key string) (dat data, err error) {
	b, err := x.r.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			err = errCacheMiss
		}
	}
	if err != nil {
		if isCacheMiss(err) {
			x.s.cacheMiss++
		} else {
			x.s.redisError++
		}
		return
	}
	x.s.cacheHit++
	if err = msgpack.Unmarshal(b, &dat); err != nil {
		x.o.log.Errorf("failed to unmarshal data: %v", err)
		x.s.dataError++
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

// load data from database to cache
// if loaded revision was accepted return "OK", else return latest data
var loadScript = NewScript(`local b=redis.call("GET", KEYS[1]);if b and cmsgpack.unpack(b).rev>cmsgpack.unpack(ARGV[1]).rev then return b end;return redis.call("SET",KEYS[1],ARGV[1],"EX",86400)`)

func (x *ReMon) load(
	ctx context.Context, key string) (dat data, err error) {
	database, collection, _id := x.o.keyMappingStrategy.MapKey(key)
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
	var s string
	if s, err = x.runScript(ctx, loadScript, key, b2s(b)).Text(); err != nil {
		return
	}
	if s != "OK" {
		if err = msgpack.Unmarshal(s2b(s), &dat); err != nil {
			x.o.log.Errorf("failed to unmarshal data: %v", err)
			x.s.dataError++
			return
		}
	}
	return
}

// update value to cache, return revision after updating
var updateScript = NewScript(`local b=redis.call("GET",KEYS[1]);if not b then error("CACHE_MISS") end;local d=cmsgpack.unpack(b);d.rev,d.val=d.rev+1,ARGV[1];local b=cmsgpack.pack(d);redis.call("SET",KEYS[1],b);if redis.call("SADD",":DIRTYSET",KEYS[1])>0 then redis.call("LPUSH",":DIRTYQUE",KEYS[1]) end;return d.rev`)

func (x *ReMon) update(ctx context.Context, key, val string) error {
	return x.runScript(ctx, updateScript, key, val).Err()
}

// [internal only] eval lua script on value
// note that an endless-loop in script will halt redis
// this method is too dangerous to export
var evalScript = NewScript(`
local b=redis.call("GET", KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
local e={cmsgpack=cmsgpack, cjson=cjson, rev=d.rev, val=d.val}
local f=loadstring(ARGV[1])
setfenv(f,e)
local r=f()
if d.val~=e.val then
d.rev,d.val=d.rev+1,e.val
local b=cmsgpack.pack(d)
redis.call("SET",KEYS[1],b)
if redis.call("SADD",":DIRTYSET",KEYS[1])>0 then redis.call("LPUSH",":DIRTYQUE",KEYS[1]) end
end
return r`)

// Eval execute a script on value, modified value will be saved automatically
// It's dangerous to eval untrusted script, so this method is only used to
// extend remon abilities
func (x *ReMon) eval(
	ctx context.Context, key, src string) (cmd *redis.Cmd) {
	if cmd = x.runScript(ctx, evalScript, key, src); isCacheMiss(cmd.Err()) {
		if _, err := x.load(ctx, key); err != nil {
			return
		}
		cmd = x.runScript(ctx, evalScript, key, src)
	}
	return
}
