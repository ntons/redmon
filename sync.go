package remon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/ntons/tongo/tunsafe"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrClosed = errors.New("remon: closed")
)

type Sync struct {
	*options

	r RedisClient
	m *mongo.Client

	closing int32
	closed  chan struct{}

	hook syncHook
}

func NewSync(r RedisClient, m *mongo.Client, opts ...Option) *Sync {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	return &Sync{
		options: o,
		r:       r,
		m:       m,
		closed:  make(chan struct{}, 1),
	}
}

func (x *Sync) ScriptLoad() (err error) {
	return ScriptLoad(x.r)
}

func (x *Sync) Hook(hookers ...SyncHooker) {
	for _, h := range hookers {
		h.apply(&x.hook)
	}
}

func (x *Sync) peekDirty() (_ string, _ _Data, err error) {
	v, err := luaPeekDirty.EvalSha(x.r, []string{}).Result()
	if err != nil {
		return
	}
	return x.getResult(v)
}

func (x *Sync) nextDirty(key string, data _Data) (_ string, _ _Data, err error) {
	v, err := luaNextDirty.EvalSha(x.r, []string{key}, data.Version,
		int64(x.volatileTTL/time.Millisecond)).Result()
	if err != nil {
		return
	}
	return x.getResult(v)
}

func (x *Sync) getResult(v interface{}) (key string, data _Data, err error) {
	a, ok := v.([]interface{})
	if !ok {
		err = fmt.Errorf("unexpected return value type: %T", v)
		return
	}
	if key, ok = a[0].(string); !ok {
		err = fmt.Errorf("unexpected return value type: %T", a[0])
		return
	}
	b, ok := a[1].(string)
	if !ok {
		err = fmt.Errorf("unexpected return value type: %T", a[1])
		return
	}
	if err = msgpack.Unmarshal(tunsafe.StringToBytes(b), &data); err != nil {
		return
	}
	return key, data, nil
}

func (x *Sync) setDataToMongo(key string, data _Data) (err error) {
	database, collection, _id := x.keyMappingStrategy.MapKey(key)
	_, err = x.m.Database(database).Collection(collection).UpdateOne(
		context.Background(),
		bson.M{"_id": _id},
		bson.M{"$set": &data},
		mongooptions.Update().SetUpsert(true),
	)
	return
}

func (x *Sync) Serve() (err error) {
	defer func() { close(x.closed) }()

	for atomic.LoadInt32(&x.closing) == 0 {
		var (
			key  string
			data _Data
		)
		if key, data, err = x.peekDirty(); err != nil {
			if err = x.onError(err); err != nil {
				return
			}
			continue
		}
		for atomic.LoadInt32(&x.closing) == 0 {
			if err = x.setDataToMongo(key, data); err != nil {
				if err = x.onError(err); err != nil {
					return
				}
				break
			}
			x.onSave(key, data.Version)
			if key, data, err = x.nextDirty(key, data); err != nil {
				if err = x.onError(err); err != nil {
					return
				}
				break
			}
		}
	}
	return
}

func (x *Sync) onSave(key string, version int64) {
	if f := x.hook.onSave; f != nil {
		f(key, version)
	}
}

func (x *Sync) onError(err error) error {
	if err == redis.Nil { // all cleaned
		time.Sleep(time.Second)
		return nil
	}
	fmt.Fprintln(os.Stderr, err)
	if f := x.hook.onError; f != nil {
		return f(err)
	} else {
		time.Sleep(time.Second)
		return nil
	}
}

func (x *Sync) Close() {
	atomic.CompareAndSwapInt32(&x.closing, 0, 1)
}

func (x *Sync) Shutdown(ctx context.Context) (err error) {
	if !atomic.CompareAndSwapInt32(&x.closing, 0, 1) {
		return ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-x.closed:
	}
	return
}
