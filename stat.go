package remon

type Stat struct {
	RedisHit   int64
	RedisMiss  int64
	RedisError int64
}
