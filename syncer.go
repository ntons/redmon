package remon

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	xDirtySet = "$DIRTYSET$"
	xDirtyQue = "$DIRTYQUE$"
)

var (
	luaSync = newScript(`
assert(#KEYS<2 and #KEYS==#ARGV)
if #KEYS>0 and redis.call("LINDEX","` + xDirtyQue + `",-1)==KEYS[1] then
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
)

type Syncer struct {
	*xOptions
	rdb RedisClient
	mdb *mongo.Client
	// life-time control
	ctx  context.Context
	stop context.CancelFunc
}

func NewSyncer(rdb RedisClient, mdb *mongo.Client, opts ...Option) *Syncer {
	o := &xOptions{}
	for _, opt := range opts {
		opt.apply(o)
	}
	ctx, stop := context.WithCancel(context.Background())
	return &Syncer{xOptions: o, rdb: rdb, mdb: mdb, ctx: ctx, stop: stop}
}

func (s *Syncer) Serve() {
	var t *time.Timer
	for {
		for k, d, err := s.peek(); ; k, d, err = s.next(k, d.Rev) {
			if err == nil {
				err = s.save(k, d)
			}
			var backoff time.Duration
			if err == nil {
				backoff = s.onSyncSave(k)
			} else if err == redis.Nil {
				backoff = s.onSyncIdle()
			} else {
				backoff = s.onSyncError(err)
			}
			if backoff > 0 {
				if t == nil {
					t = time.NewTimer(backoff)
					defer t.Stop()
				} else {
					t.Reset(backoff)
				}
				select {
				case <-s.ctx.Done():
					return
				case <-t.C:
				}
			}
			if err != nil {
				break
			}
		}
	}
}

func (s *Syncer) Stop() { s.stop() }

// peek top dirty key and data
func (s *Syncer) peek() (string, xRedisData, error) {
	return s.resolve(luaSync.Run(s.ctx, s.rdb, []string{}))
}

// clean dirty flag and make key volatile, then peek the next
func (s *Syncer) next(key string, rev int64) (string, xRedisData, error) {
	return s.resolve(luaSync.Run(s.ctx, s.rdb, []string{key}, rev))
}

func (*Syncer) resolve(
	r *redis.Cmd) (_ string, data xRedisData, err error) {
	var v interface{}
	if v, err = r.Result(); err != nil {
		return
	} else if a, ok := v.([]interface{}); !ok || len(a) != 2 {
		panic(fmt.Errorf("unexpected return type: %T", r))
	} else if err = msgpack.Unmarshal(
		fastStringToBytes(a[1].(string)), &data); err != nil {
		return
	} else {
		return a[0].(string), data, nil
	}
}

func (s *Syncer) save(key string, data xRedisData) (err error) {
	database, collection, _id := s.mapKey(key)
	_, err = s.mdb.Database(database).Collection(collection).UpdateOne(
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
