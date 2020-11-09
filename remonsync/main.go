package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
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
	*r = append(*r, url)
	return
}

func Dial() (err error) {
	for _, url := range cfg.RedisURLs {
		o, err := redis.ParseURL(url)
		if err != nil {
			return fmt.Errorf("invalid redis url: %s", url)
		}
		rClients = append(rClients, redis.NewClient(o))
	}
	if url := string(cfg.MongoURL); true {
		o := mongooptions.Client().ApplyURI(url)
		if mClient, err = mongo.NewClient(o); err != nil {
			return fmt.Errorf("invalid redis url: %s", url)
		}
		if err = mClient.Connect(context.Background()); err != nil {
			return fmt.Errorf("failed to connect mongo: %s", err)
		}
	}
	return
}

func Serve() {
	var (
		wg    sync.WaitGroup
		syncs []*remon.Sync
	)
	defer func() {
		for _, sync := range syncs {
			sync.Close()
		}
		wg.Wait()
	}()
	for _, rClient := range rClients {
		sync := remon.NewSync(rClient, mClient, remon.WithSyncLimit(cfg.Limit))
		if err := remon.ScriptLoad(rClient); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		sync.Hook(remon.SyncOnSave(func(key string, version int64) {
			log.Debugf("save: %s %d", key, version)
		}))
		sync.Hook(remon.SyncOnError(func(err error) error {
			log.Error(err)
			time.Sleep(time.Second)
			return nil
		}))
		syncs = append(syncs, sync)
		wg.Add(1)
		go func() { defer wg.Done(); sync.Serve() }()
	}

	quit := make(chan os.Signal, 1)
	signal.Ignore(syscall.SIGPIPE)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

type Config struct {
	// options
	Limit     int
	Verbose   bool
	RedisURLs RedisURLs
	MongoURL  string
}

var cfg Config

func main() {
	flag.IntVar(&cfg.Limit, "limit", 0, "sync count limit per second")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "verbose log")
	flag.Var(&cfg.RedisURLs, "r", "redis url")
	flag.Var(&cfg.RedisURLs, "redis", "redis url")
	flag.StringVar(&cfg.MongoURL, "m", "", "mongo url")
	flag.StringVar(&cfg.MongoURL, "mongo", "", "mongo url")
	flag.Parse()
	if len(flag.Args()) > 0 {
		if b, err := ioutil.ReadFile(flag.Args()[0]); err != nil {
			log.Fatal("failed to open config file: %s", flag.Args()[0])
		} else if err := json.Unmarshal(b, &cfg); err != nil {
			log.Fatal("failed to parse config file: %s", err)
		}
	}
	flag.Parse() //parse again to prefer command line

	logOpts := []log.ConsoleLoggerOption{
		log.ConsoleLoggerWithColor(),
	}
	if cfg.Verbose {
		logOpts = append(logOpts, log.ConsoleLoggerWithLevel(log.DebugLevel))
	}
	log.Init(log.NewConsoleLogger(logOpts...))

	if len(cfg.RedisURLs) == 0 {
		log.Fatal("at least one redis url should be specified")
	}
	if len(cfg.MongoURL) == 0 {
		log.Fatal("mongo url should be specified")
	}
	if err := Dial(); err != nil {
		log.Fatal("failed to dial: ", err)
	}
	Serve()
}
