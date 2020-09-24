package main

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v7"
	log "github.com/ntons/log-go"
	"go.mongodb.org/mongo-driver/mongo"
	options "go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"
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

func Dial() (r *redis.Client, m *mongo.Client, err error) {
	r = redis.NewClient(&redis.Options{Addr: cfg.Redis})
	if m, err = mongo.NewClient(
		options.Client().ApplyURI(cfg.Mongo)); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = m.Connect(ctx); err != nil {
		return
	}
	return
}

/*
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

}
*/

type SyncConfig struct {
	Rate int `yaml:"rate"`
}
type Config struct {
	Redis string     `yaml:"redis"`
	Mongo string     `yaml:"mongo"`
	Sync  SyncConfig `yaml:"sync"`
}

var cfg Config

func main() {
	defer log.Sync()

	var fp string
	flag.StringVar(&fp, "c", "", "configuration file path")
	flag.StringVar(&cfg.Redis, "redis", "", "redis connection url")
	flag.StringVar(&cfg.Mongo, "mongo", "", "mongo connection url")
	flag.IntVar(&cfg.Sync.Rate, "rate", 0, "sync rate")
	flag.Parse()
	if len(flag.Args()) == 0 {
		log.Fatal("require command")
	}
	if fp != "" {
		if b, err := ioutil.ReadFile(fp); err != nil {
			log.Fatalf("failed to read file: %v", err)
		} else if err := yaml.Unmarshal(b, &cfg); err != nil {
			log.Fatalf("failed to unmarshal file: %v", err)
		}
	}
	flag.Parse()

	log.Debugf("config: %#v", cfg)

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch flag.Args()[0] {
	case "sync":
		wg.Add(1)
		go func() {
			defer func() { cancel(); wg.Done() }()
			Sync(ctx)
		}()
	case "inspect":
		wg.Add(1)
		go func() {
			defer func() { cancel(); wg.Done() }()
			Inspect(ctx, flag.Args()[1:])
		}()
	}

	quit := make(chan os.Signal, 1)
	signal.Ignore(syscall.SIGPIPE)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
		cancel()
	case <-ctx.Done():
	}
}
