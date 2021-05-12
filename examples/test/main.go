// 测试功能是否正确

// 本地存储10000条数据，每条数据平均长度在5K-15K之间随机
// 并发M个协程更新DB的同时更新本地数据
// N条数据的访问权重为其序号，序号越大访问越频繁
// 在经过T时间后，验证DB数据与本地数据是否一致

package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	mrand "math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/remon"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Data struct {
	Weight  int
	Rev     int64
	Payload string
}

var (
	db     remon.Client
	syncer *remon.Syncer

	localData      = make([]*Data, 0)
	localDataMutex []sync.Mutex

	totalWeight = 0

	counter int64 = 0
)

func Dial() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	mdb, _ := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mdb.Connect(ctx)
	db = remon.New(rdb, mdb)
	syncer = remon.NewSyncer(rdb, mdb)
}

func RandPayload() string {
	b := make([]byte, 500+mrand.Intn(1000))
	crand.Read(b)
	return string(b)
}

func InitLocalData() {
	const N = 10000
	for i := 0; i < N; i++ {
		totalWeight += i
		localData = append(localData, &Data{
			Weight: totalWeight,
		})
	}
	localDataMutex = make([]sync.Mutex, N)
}

func RandIndex() (i int) {
	k := mrand.Intn(totalWeight)
	return sort.Search(
		len(localData), func(i int) bool {
			return localData[i].Weight >= k
		},
	)
}

func main() {

	InitLocalData()

	Dial()

	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	go func() { syncer.Serve() }()

	// 并发测试
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				i := RandIndex()
				localDataMutex[i].Lock()
				d := localData[i]
				v := RandPayload()
				_ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				rev, err := db.Set(_ctx, fmt.Sprintf("%d", i), v)
				if err != nil {
					fmt.Printf("failed to set data %d: %s\n", i, err)
					localDataMutex[i].Unlock()
					continue
				}
				d.Rev = rev
				d.Payload = v
				localDataMutex[i].Unlock()
				if n := atomic.AddInt64(&counter, 1); n%1000000 == 0 {
					fmt.Printf("count: %d\n", n)
				}
			}
		}()
	}

	wg.Wait()

	fmt.Printf("Count: %d\n", counter)
	fmt.Printf("CacheHit:   %d\n", db.Metrics().CacheHit())
	fmt.Printf("CacheMiss:  %d\n", db.Metrics().CacheMiss())
	fmt.Printf("RedisError: %d\n", db.Metrics().RedisError())
	fmt.Printf("MongoError: %d\n", db.Metrics().MongoError())
	fmt.Printf("DataError:  %d\n", db.Metrics().DataError())

	// 检查数据完整性
	for i := 0; i < len(localData); i++ {
		_ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		d := localData[i]
		if d.Payload == "" {
			continue
		}
		rev, val, err := db.Get(_ctx, fmt.Sprintf("%d", i))
		if err != nil {
			fmt.Printf("failed to get data: %d, %v\n", i, err)
			continue
		}
		if val != d.Payload || rev != d.Rev {
			fmt.Printf("data mismatch: %d, %d, %d, %d, %d\n", i, rev, d.Rev, len(val), len(d.Payload))
		}
	}

	syncer.Stop()

}
