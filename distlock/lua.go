package redislock

import (
	"errors"
	"fmt"

	"github.com/go-redis/redis/v7"
)

const (
	luaOK          = 0
	luaNotLocked   = -1
	luaLockNotHeld = -2
)

var (
	ErrNotLocked   = errors.New("redislock: not locked")
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

func luaRetToErr(luaRet int64) (err error) {
	if luaRet >= luaOK {
		return
	}
	switch luaRet {
	case luaNotLocked:
		return ErrNotLocked
	case luaLockNotHeld:
		return ErrLockNotHeld
	default:
		return fmt.Errorf("remon: unknown error code: %d", luaRet)
	}
}

var (
	luaLock = redis.NewScript(`
local token = redis.call("GET", KEYS[1])
if token then return { token, redis.call("PTTL", KEYS[1]) } end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")
return ARGV`)

	luaUnlock = redis.NewScript(`
local token = redis.call("GET", KEYS[1])
if not token then return -1 end
if token ~= ARGV[1] then return -2 end
redis.call("DEL", KEYS[1])
return 0`)

	luaCheck = redis.NewScript(`
local token = redis.call("GET", KEYS[1])
if not token then return -1 end
if token ~= ARGV[1] then return -2 end
return redis.call("PTTL")`)

	luaRefresh = redis.NewScript(`
local token = redis.call("GET", KEYS[1])
if not token then return -1 end
if token ~= ARGV[1] then return -2 end
redis.call("PEXPIRE", ARGV[2])
return 0`)
)

func ScriptLoad(r RedisClient) (err error) {
	scripts := []*redis.Script{
		luaLock,
		luaUnlock,
		luaCheck,
		luaRefresh,
	}
	for _, script := range scripts {
		if _, err = script.Load(r).Result(); err != nil {
			return
		}
	}
	return
}
