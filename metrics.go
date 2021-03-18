package remon

import (
	"sync/atomic"
)

// client wide statistics
type Metrics struct {
	cacheHit   int64 // cache hit counter
	cacheMiss  int64 // cache miss counter
	redisError int64 // redis error counter
	mongoError int64 // mongo error counter
	dataError  int64 // data marshal/unmarshal error counter
}

func (m *Metrics) CacheHit() int64   { return atomic.LoadInt64(&m.cacheHit) }
func (m *Metrics) CacheMiss() int64  { return atomic.LoadInt64(&m.cacheMiss) }
func (m *Metrics) RedisError() int64 { return atomic.LoadInt64(&m.redisError) }
func (m *Metrics) MongoError() int64 { return atomic.LoadInt64(&m.mongoError) }
func (m *Metrics) DataError() int64  { return atomic.LoadInt64(&m.dataError) }

func (m *Metrics) incrCacheHit()   { atomic.AddInt64(&m.cacheHit, 1) }
func (m *Metrics) incrCacheMiss()  { atomic.AddInt64(&m.cacheMiss, 1) }
func (m *Metrics) incrRedisError() { atomic.AddInt64(&m.redisError, 1) }
func (m *Metrics) incrMongoError() { atomic.AddInt64(&m.mongoError, 1) }
func (m *Metrics) incrDataError()  { atomic.AddInt64(&m.dataError, 1) }
