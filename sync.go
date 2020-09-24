package remon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

type Sync struct {
	o *options
	r redisClient
	m *mongo.Client
	// life-time control
	ctx    context.Context
	cancel context.CancelFunc
	// rate limit
	counter int32
	cond    *sync.Cond
}

func NewSync(r redisClient, m *mongo.Client, opts ...Option) *Sync {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	x := &Sync{o: o, r: r, m: m}
	x.ctx, x.cancel = context.WithCancel(context.Background())
	return x
}

func (x *Sync) Close() { x.cancel() }

func (x *Sync) checkRate() {
}

func (x *Sync) Serve() {
	var tk *time.Ticker // rate limit beat generater
	if x.o.rate > 0 {
		tk = time.NewTicker(time.Second / time.Duration(x.o.rate))
		defer tk.Stop()
	}
	for {
		k, d, err := x.peek()
		for ; err == nil; k, d, err = x.next(k, d.Rev) {
			if err = x.save(k, d); err != nil {
				break
			}
			x.o.log.Debugf("sync: %s saved", k)
			if tk != nil {
				select {
				case <-x.ctx.Done():
					return
				case <-tk.C:
				}
			}
		}
		if err != redis.Nil {
			x.o.log.Errorf("failed to sync: %v", err)
		}
		// no matter no dirty data or other error, halt 1 second
		select {
		case <-x.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// peek top dirty key and data
var peekScript = NewScript(`local k=redis.call("LINDEX",":DIRTYQUE",-1);if not k then return end;local b = redis.call("GET",k);if not b then redis.call("RPOP",":DIRTYQUE");redis.call("SREM",":DIRTYSET",k);return end;return {k,b}`)

func (x *Sync) peek() (_ string, _ data, err error) {
	return x.runScript(peekScript, []string{})
}

// clean dirty flag and make key volatile, then peek the next
var nextScript = NewScript(`
if redis.call("LINDEX",":DIRTYQUE",-1)==KEYS[1] then
    local b=redis.call("GET",KEYS[1])
    if not b then
        redis.call("RPOP",":DIRTYQUE")
        redis.call("SREM",":DIRTYSET",KEYS[1])
    else
        local d=cmsgpack.unpack(b)
        if tostring(d.rev)==ARGV[1] then
            redis.call("RPOP",":DIRTYQUE")
            redis.call("SREM",":DIRTYSET",KEYS[1])
            redis.call("EXPIRE",KEYS[1],86400)
        else
            redis.call("RPOPLPUSH",":DIRTYQUE",":DIRTYQUE")
        end
    end
end
` + peekScript.src)

func (x *Sync) next(key string, rev int64) (_ string, _ data, err error) {
	return x.runScript(nextScript, []string{key}, rev)
}

func (x *Sync) runScript(
	script *Script, keys []string, args ...interface{}) (
	_ string, dat data, err error) {
	v, err := script.RunContext(x.ctx, x.r, keys, args...).Result()
	if err == redis.Nil {
		// nothing to peek
		return
	}
	a, ok := v.([]interface{})
	if !ok || len(a) != 2 {
		panic(fmt.Errorf("unexpected return type: %T", v))
	}
	if err = msgpack.Unmarshal(s2b(a[1].(string)), &dat); err != nil {
		return
	}
	return a[0].(string), dat, nil
}

func (x *Sync) save(key string, dat data) (err error) {
	database, collection, _id := x.o.keyMappingStrategy.MapKey(key)
	_, err = x.m.Database(database).Collection(collection).UpdateOne(
		context.Background(),
		bson.M{"_id": _id},
		bson.M{"$set": &dat},
		mongooptions.Update().SetUpsert(true),
	)
	return
}
