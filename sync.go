package remon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
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
	// options
	o *options
	// redis client
	r RedisClient
	// mongo client
	m *mongo.Client

	closing int32
	closed  chan struct{}

	hook syncHook

	limitCounter int32
	limitCond    *sync.Cond
}

func NewSync(r RedisClient, m *mongo.Client, opts ...Option) *Sync {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	return &Sync{
		o:      o,
		r:      r,
		m:      m,
		closed: make(chan struct{}, 1),
	}
}

func (x *Sync) Hook(hookers ...SyncHooker) {
	for _, h := range hookers {
		h.apply(&x.hook)
	}
}

func (x *Sync) Serve() (err error) {
	defer func() { close(x.closed) }()

	if x.o.syncLimit > 0 {
		x.limitCond = sync.NewCond(&sync.Mutex{})
		t := time.NewTicker(time.Second)
		q := make(chan struct{}, 1)

		var wg sync.WaitGroup
		defer func() { t.Stop(); close(q); wg.Wait() }()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-q:
					return
				case <-t.C:
					atomic.StoreInt32(&x.limitCounter, 0)
					x.limitCond.Broadcast()
				}
			}
		}()
	}

	for atomic.LoadInt32(&x.closing) == 0 {
		var (
			key  string
			data Data
		)
		if key, data, err = x.peekDirty(); err != nil {
			if err = x.onError(err); err != nil {
				return
			}
			continue
		}
		for atomic.LoadInt32(&x.closing) == 0 {
			if err = x.saveDataToMongo(key, data); err != nil {
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

func (x *Sync) Close() {
	atomic.CompareAndSwapInt32(&x.closing, 0, 1)
}

// close sync gracefully, waiting all write complete
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

func (x *Sync) peekDirty() (_ string, _ Data, err error) {
	var v interface{}
	if v, err = x.eval(
		context.Background(), luaPeekDirty, []string{},
	).Result(); err != nil {
		return
	}
	return x.getResult(v)
}

func (x *Sync) nextDirty(key string, data Data) (_ string, _ Data, err error) {
	var v interface{}
	if v, err = x.eval(
		context.Background(), luaNextDirty, []string{key},
		data.Version, int64(x.o.volatileTTL/time.Millisecond),
	).Result(); err != nil {
		return
	}
	return x.getResult(v)
}

func (x *Sync) eval(ctx context.Context, lua *redis.Script, keys []string, args ...interface{}) (cmd *redis.Cmd) {
	cmdArgs := make([]interface{}, 3, 3+len(keys)+len(args))
	cmdArgs[0] = "evalsha"
	cmdArgs[1] = lua.Hash()
	cmdArgs[2] = len(keys)
	for _, key := range keys {
		cmdArgs = append(cmdArgs, key)
	}
	cmdArgs = append(cmdArgs, args...)
	cmd = redis.NewCmd(cmdArgs...)

	err := x.r.ProcessContext(ctx, cmd)
	for isBusyError(err) {
		pipe := x.r.Pipeline()
		pipe.ScriptKill()
		cmd = pipe.EvalSha(lua.Hash(), keys, args...)
		pipe.ExecContext(ctx)
		err = cmd.Err()
	}
	return
}

func (x *Sync) getResult(v interface{}) (key string, data Data, err error) {
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

func (x *Sync) saveDataToMongo(key string, data Data) (err error) {
	database, collection, _id := x.o.keyMappingStrategy.MapKey(key)
	_, err = x.m.Database(database).Collection(collection).UpdateOne(
		context.Background(),
		bson.M{"_id": _id},
		bson.M{"$set": &data},
		mongooptions.Update().SetUpsert(true),
	)
	return
}

func (x *Sync) onSave(key string, version int64) {
	if x.o.syncLimit > 0 {
		x.limitCond.L.Lock()
		if atomic.AddInt32(&x.limitCounter, 1) > x.o.syncLimit {
			x.limitCond.Wait()
		}
		x.limitCond.L.Unlock()
	}
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
