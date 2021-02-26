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

type SyncClient struct {
	o *xOptions
	r RedisClient
	m *mongo.Client
	// life-time control
	ctx  context.Context
	stop context.CancelFunc
	// rate limit
	counter int32
	cond    *sync.Cond
}

func NewSyncClient(r RedisClient, m *mongo.Client, opts ...Option) *SyncClient {
	o := newOptions()
	for _, opt := range opts {
		opt.apply(o)
	}
	ctx, stop := context.WithCancel(context.Background())
	return &SyncClient{o: o, r: r, m: m, ctx: ctx, stop: stop}
}
func NewSync(r RedisClient, m *mongo.Client, opts ...Option) *SyncClient {
	return NewSyncClient(r, m, opts...)
}

func (cli *SyncClient) Serve() {
	var tk *time.Ticker // rate limit beat generater
	if cli.o.syncRate > 0 {
		tk = time.NewTicker(time.Second / time.Duration(cli.o.syncRate))
		defer tk.Stop()
	}
	for {
		key, data, err := cli.peek()
		for ; err == nil; key, data, err = cli.next(key, data.Rev) {
			if err = cli.save(key, data); err != nil {
				break
			}
			cli.o.log.Debugf("sync: %s saved", key)
			if tk != nil {
				select {
				case <-cli.ctx.Done():
					return
				case <-tk.C:
				}
			}
		}
		if err != redis.Nil {
			cli.o.log.Errorf("failed to sync: %v", err)
		}
		// no dirty data or other error, halt 1 second
		select {
		case <-cli.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (cli *SyncClient) Stop() { cli.stop() }

// peek top dirty key and data
func (cli *SyncClient) peek() (_ string, _ xData, err error) {
	return cli.runScript(peekScript, []string{})
}

// clean dirty flag and make key volatile, then peek the next
func (cli *SyncClient) next(
	key string, rev int64) (_ string, _ xData, err error) {
	return cli.runScript(nextScript, []string{key}, rev)
}

func (cli *SyncClient) runScript(
	script *Script, keys []string, args ...interface{}) (
	_ string, data xData, err error) {
	v, err := script.Run(cli.ctx, cli.r, keys, args...).Result()
	if err == redis.Nil {
		return // nothing to peek
	}
	a, ok := v.([]interface{})
	if !ok || len(a) != 2 {
		panic(fmt.Errorf("unexpected return type: %T", v))
	}
	if err = msgpack.Unmarshal(s2b(a[1].(string)), &data); err != nil {
		return
	}
	return a[0].(string), data, nil
}

func (cli *SyncClient) save(key string, data xData) (err error) {
	database, collection, _id := cli.o.keyMappingStrategy.MapKey(key)
	_, err = cli.m.Database(database).Collection(collection).UpdateOne(
		context.Background(),
		bson.M{"_id": _id},
		bson.M{"$set": &data},
		options.Update().SetUpsert(true),
	)
	return
}
