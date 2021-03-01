package remon

import (
	"sync/atomic"
)

// client wide statistics
type Stat struct {
	cacheHit   int64 // cache hit counter
	cacheMiss  int64 // cache miss counter
	redisError int64 // redis error counter
	mongoError int64 // mongo error counter
	dataError  int64 // data marshal/unmarshal error counter
}

func (s *Stat) CacheHit() int64   { return atomic.LoadInt64(&s.cacheHit) }
func (s *Stat) CacheMiss() int64  { return atomic.LoadInt64(&s.cacheMiss) }
func (s *Stat) RedisError() int64 { return atomic.LoadInt64(&s.redisError) }
func (s *Stat) MongoError() int64 { return atomic.LoadInt64(&s.mongoError) }
func (s *Stat) DataError() int64  { return atomic.LoadInt64(&s.dataError) }

func (s *Stat) incrCacheHit()   { atomic.AddInt64(&s.cacheHit, 1) }
func (s *Stat) incrCacheMiss()  { atomic.AddInt64(&s.cacheMiss, 1) }
func (s *Stat) incrRedisError() { atomic.AddInt64(&s.redisError, 1) }
func (s *Stat) incrMongoError() { atomic.AddInt64(&s.mongoError, 1) }
func (s *Stat) incrDataError()  { atomic.AddInt64(&s.dataError, 1) }
