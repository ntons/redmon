package remon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	luaPeek = newScript(`
local k=redis.call("LINDEX","` + xDirtyQue + `",-1)
if not k then return end
local b=redis.call("GET",k)
if not b then
  redis.call("RPOP","` + xDirtyQue + `")
  redis.call("SREM","` + xDirtySet + `",k)
  return
end
return {k,b}
`)

	luaNext = newScript(`
if redis.call("LINDEX","` + xDirtyQue + `",-1)==KEYS[1] then
  local b=redis.call("GET",KEYS[1])
  if not b then
    redis.call("RPOP","` + xDirtyQue + `")
    redis.call("SREM","` + xDirtySet + `",KEYS[1])
  else
    local d=cmsgpack.unpack(b)
    if tostring(d.rev)==ARGV[1] then
      redis.call("RPOP","` + xDirtyQue + `")
      redis.call("SREM","` + xDirtySet + `",KEYS[1])
      redis.call("EXPIRE",KEYS[1],86400)
    else
      redis.call("RPOPLPUSH","` + xDirtyQue + `","` + xDirtyQue + `")
    end
  end
end
` + luaPeek.src)
)

type Syncer struct {
	opts *xOptions
	rdb  RedisClient
	mdb  *mongo.Client
	// life-time control
	ctx  context.Context
	stop context.CancelFunc
	// rate limit
	counter int32
	cond    *sync.Cond
}

func NewSyncer(
	rdb RedisClient, mdb *mongo.Client, opts ...Option) *Syncer {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	ctx, stop := context.WithCancel(context.Background())
	return &Syncer{opts: o, rdb: rdb, mdb: mdb, ctx: ctx, stop: stop}
}

func (syncer *Syncer) Serve() {
	var tick *time.Ticker // rate limit beat generater
	if syncer.opts.syncRate > 0 {
		tick = time.NewTicker(time.Second / time.Duration(syncer.opts.syncRate))
		defer tick.Stop()
	}
	for {
		key, data, err := syncer.peek()
		for ; err == nil; key, data, err = syncer.next(key, data.Rev) {
			if err = syncer.save(key, data); err != nil {
				break
			}
			syncer.opts.log.Debugf("sync: %s saved", key)
			if tick != nil {
				select {
				case <-syncer.ctx.Done():
					return
				case <-tick.C:
				}
			}
		}
		if err != redis.Nil {
			syncer.opts.log.Errorf("failed to sync: %v", err)
		}
		// no dirty data or other error, halt 1 second
		select {
		case <-syncer.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (syncer *Syncer) Stop() { syncer.stop() }

// peek top dirty key and data
func (syncer *Syncer) peek() (string, xRedisData, error) {
	return syncer.runScript(luaPeek, "", 0)
}

// clean dirty flag and make key volatile, then peek the next
func (syncer *Syncer) next(key string, rev int64) (string, xRedisData, error) {
	return syncer.runScript(luaNext, key, rev)
}

func (syncer *Syncer) runScript(script *xScript, key string, rev int64) (
	_ string, data xRedisData, err error) {
	var (
		keys []string
		args []interface{}
	)
	if key != "" && rev > 0 {
		keys, args = []string{key}, []interface{}{rev}
	}
	r, err := script.Run(syncer.ctx, syncer.rdb, keys, args...).Result()
	if err != nil {
		return
	}
	a, ok := r.([]interface{})
	if !ok || len(a) != 2 {
		panic(fmt.Errorf("unexpected return type: %T", r))
	}
	if err = msgpack.Unmarshal(
		fastStringToBytes(a[1].(string)), &data); err != nil {
		return
	}
	return a[0].(string), data, nil
}

func (syncer *Syncer) save(key string, data xRedisData) (err error) {
	database, collection, _id := syncer.opts.keyMappingStrategy.MapKey(key)
	_, err = syncer.mdb.Database(database).Collection(collection).UpdateOne(
		context.Background(),
		bson.M{"_id": _id},
		bson.M{"$set": &xMongoData{
			Rev: data.Rev,
			Val: fastStringToBytes(data.Val),
		}},
		options.Update().SetUpsert(true),
	)
	return
}
