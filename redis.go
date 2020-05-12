package remon

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

type RedisScripter interface { // redis.scripter
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(script string) *redis.StringCmd
}

type RedisClient interface {
	RedisScripter

	Ping() *redis.StatusCmd
	Get(key string) *redis.StringCmd
	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
}

type RedisClientWithContext interface {
	RedisClient
	WithContext(ctx context.Context) RedisClient
}

func NewRedisClientWithContext(r interface{}) RedisClientWithContext {
	switch r.(type) {
	case *redis.Client:
		return &redisClient{r.(*redis.Client)}
	case *redis.ClusterClient:
		return &redisClusterClient{r.(*redis.ClusterClient)}
	default:
		panic(fmt.Errorf("unsupported redis client type: %T", r))
	}
}

type redisClient struct {
	*redis.Client
}

func (c *redisClient) WithContext(ctx context.Context) RedisClient {
	return &redisClient{Client: c.Client.WithContext(ctx)}
}

type redisClusterClient struct {
	*redis.ClusterClient
}

func (c *redisClusterClient) WithContext(ctx context.Context) RedisClient {
	return &redisClusterClient{ClusterClient: c.ClusterClient.WithContext(ctx)}
}
