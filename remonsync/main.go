package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/ntons/log-go"
	"github.com/ntons/remon"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

var (
	// runtime variables
	rClients []*redis.Client
	mClient  *mongo.Client

	limitCnt  int32
	limitCond *sync.Cond
)

type RedisURLs []string

func (r *RedisURLs) String() string {
	s := ""
	for _, url := range *r {
		if s != "" {
			s += ","
		}
		s += url
	}
	return s
}
func (r *RedisURLs) Set(url string) (err error) {
	i := sort.Search(len(*r), func(i int) bool {
		return (*r)[i] >= url
	})
	if i < len(*r) && (*r)[i] == url {
		return nil // dup
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	rClients = append(rClients, redis.NewClient(opts))
	*r = append(*r, "")
	copy((*r)[i+1:], (*r)[i:])
	(*r)[i] = url

	return nil
}

type MongoURL string

func (m *MongoURL) String() string {
	return string(*m)
}
func (m *MongoURL) Set(url string) (err error) {
	if mClient != nil {
		mClient.Disconnect(context.Background())
	}
	opts := mongooptions.Client().ApplyURI(url)
	if mClient, err = mongo.NewClient(opts); err != nil {
		return
	}
	if err = mClient.Connect(context.Background()); err != nil {
		return
	}
	*m = MongoURL(url)
	return nil
}

func OnSave(key string, version int64) {
	log.Infof("save: %s %d", key, version)
	if limit > 0 {
		limitCond.L.Lock()
		if atomic.AddInt32(&limitCnt, 1) > int32(limit) {
			limitCond.Wait()
		}
		limitCond.L.Unlock()
	}
}

func OnError(err error) error {
	log.Error(err)
	time.Sleep(time.Second)
	return nil
}

func Serve() {
	var wg sync.WaitGroup
	defer wg.Wait()

	if limit > 0 {
		limitCond = sync.NewCond(&sync.Mutex{})
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tk.C {
				atomic.StoreInt32(&limitCnt, 0)
				limitCond.Broadcast()
			}
		}()
	}

	syncs := make([]*remon.Sync, 0)
	defer func() {
		for _, sync := range syncs {
			sync.Close()
		}
	}()

	for _, rClient := range rClients {
		sync := remon.NewSync(rClient, mClient)
		if err := sync.ScriptLoad(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		sync.Hook(remon.SyncOnSave(OnSave))
		sync.Hook(remon.SyncOnError(OnError))
		syncs = append(syncs, sync)
		wg.Add(1)
		go func() { defer wg.Done(); sync.Serve() }()
	}

	quit := make(chan os.Signal, 1)
	signal.Ignore(syscall.SIGPIPE)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

var (
	// options
	limit   int       // -limit
	verbose bool      // -verbose
	rURLs   RedisURLs // -reids
	mURL    MongoURL  // -mongo
)

func main() {
	flag.IntVar(&limit, "limit", 0, "sync count limit per second")
	flag.BoolVar(&verbose, "verbose", false, "verbose log")
	flag.Var(&rURLs, "r", "redis url")
	flag.Var(&rURLs, "redis", "redis url")
	flag.Var(&mURL, "m", "mongo url")
	flag.Var(&mURL, "mongo", "mongo url")
	flag.Parse()

	logOpts := []log.ConsoleLoggerOption{
		log.ConsoleLoggerWithColor(),
	}
	if verbose {
		logOpts = append(logOpts, log.ConsoleLoggerWithLevel(log.DebugLevel))
	}
	log.Init(log.NewConsoleLogger(logOpts...))

	if len(rURLs) == 0 {
		log.Fatal("at least one redis url should be specified")
	}
	if len(mURL) == 0 {
		log.Fatal("mongo url should be specified")
	}
	Serve()
}
