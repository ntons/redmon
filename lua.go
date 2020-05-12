package remon

import (
	"github.com/go-redis/redis/v7"
)

var (
	// load cache from mongo
	// newer version will be accepted while cache data exists
	// KEYS   = { KEY }
	// ARGV   = { BUF }
	// RETURN = BUF
	luaLoadData = redis.NewScript(`
local buf = redis.call("GET", KEYS[1])
if buf and cmsgpack.unpack(buf).version >= cmsgpack.unpack(ARGV[1]).version then return buf end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return ARGV[1]`)

	// KEYS   = { KEY }
	// ARGV   = { VALUE }
	// RETURN = nil
	luaSetValue = redis.NewScript(`
local data = { ["version"] = 0 }
local buf = redis.call("GET", KEYS[1])
if buf then data = cmsgpack.unpack(buf) end
if data.value ~= ARGV[1] then
    data.version = data.version + 1
	data.value = ARGV[1]
	redis.call("SET", KEYS[1], cmsgpack.pack(data))
	if redis.call("SADD", ":DIRTYSET", KEYS[1]) > 0 then
		redis.call("LPUSH", ":DIRTYQUE", KEYS[1])
	end
end`)

	// peek the first dirty record
	// KEYS   = {}
	// ARGV   = {}
	// RETURN = nil | { KEY, BUF }
	luaPeekDirtySrc = `
local key = redis.call("LINDEX", ":DIRTYQUE", -1)
if not key then return nil end
local buf = redis.call("GET", key)
if not buf then
	redis.call("RPOP", ":DIRTYQUE")
	redis.call("SREM", ":DIRTYSET", key)
	return nil
end
return { key, buf }`
	luaPeekDirty = redis.NewScript(luaPeekDirtySrc)

	// clean the first dirty record then get the next
	// KEYS   = { KEY }
	// ARGV   = { VERSION, TTL }
	// RETURN = nil | { KEY, BUF }
	luaNextDirty = redis.NewScript(`
if redis.call("LINDEX",":DIRTYQUE", -1) == KEYS[1] then
    local buf = redis.call("GET", KEYS[1])
	if not buf then
		redis.call("RPOP", ":DIRTYQUE")
		redis.call("SREM", ":DIRTYSET", KEYS[1])
	else
		local data = cmsgpack.unpack(buf)
		if tostring(data.version) == ARGV[1] then
			redis.call("RPOP", ":DIRTYQUE")
			redis.call("SREM", ":DIRTYSET", KEYS[1])
			redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			redis.call("RPOPLPUSH", ":DIRTYQUE", ":DIRTYQUE")
		end
	end
end` + luaPeekDirtySrc)
)

func ScriptLoad(rdb RedisClient) (err error) {
	scripts := []*redis.Script{
		luaLoadData,
		luaSetValue,
		luaPeekDirty,
		luaNextDirty,
	}
	for _, script := range scripts {
		if _, err = script.Load(rdb).Result(); err != nil {
			return
		}
	}
	return
}
