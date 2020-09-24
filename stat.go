package remon

// remon statistics
type Stat struct {
	cacheHit   int64
	cacheMiss  int64
	redisError int64
	mongoError int64
	dataError  int64
}

// redis cache hit rate
func (s *Stat) HitRate() float64 {
	return float64(s.cacheHit) / float64(s.cacheHit+s.cacheMiss)
}

// redis cache miss rate
func (s *Stat) MissRate() float64 {
	return float64(s.cacheMiss) / float64(s.cacheHit+s.cacheMiss)
}

// redis error count, cache miss is not included
func (s *Stat) RedisErrorCount() int64 {
	return s.redisError
}

// mongo error count, not found is not included
func (s *Stat) MongoErrorCount() int64 {
	return s.mongoError
}

// data marshal/unmarshal error count
func (s *Stat) DataErrorCount() int64 {
	return s.dataError
}
