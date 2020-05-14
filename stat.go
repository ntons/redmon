package remon

type Stat struct {
	RedisHit   int64 // cache hit counter
	RedisMiss  int64 // cache miss counter
	RedisError int64 // cache error counter
	MongoError int64 // db error counter
	DataError  int64 // data error counter
}
