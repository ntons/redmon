package remon

import (
	"github.com/go-redis/redis/v7"
)

type scripter interface { // redis.scripter
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(script string) *redis.StringCmd
}

var scripts []*redis.Script

func newScript(src string) *redis.Script {
	script := redis.NewScript(src)
	scripts = append(scripts, script)
	return script
}

func ScriptLoad(r scripter) (err error) {
	for _, script := range scripts {
		if _, err = script.Load(r).Result(); err != nil {
			return
		}
	}
	return
}

var (
	luaDefDataEnc = `
local encode_data(data) = function()
    return cmsgpack.pack(data)
end`
	luaDefGetData = `
local get_data = function()
    local buf = redis.call("GET", KEYS[1])
    if not buf then return nil end
    return cmsgpack.unpack(buf)
end`
	luaDefSetData = `
local set_data = function(data)
    data.version = data.version + 1
    redis.call("SET", KEYS[1], cmsgpack.pack(data), "XX")
    if redis.call("SADD", ":DIRTYSET", KEYS[1]) > 0 then
        redis.call("LPUSH", ":DIRTYQUE", KEYS[1])
    end
end`
	// load cache from mongo
	// newer version will be accepted while cache data exists
	// KEYS   = { KEY }
	// ARGV   = { BUF, PX }
	// RETURN = BUF
	luaLoadData = newScript(`
local buf = redis.call("GET", KEYS[1])
if buf and cmsgpack.unpack(buf).version >= cmsgpack.unpack(ARGV[1]).version then return buf end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return ARGV[1]`)

	// KEYS   = { KEY }
	// ARGV   = { VALUE }
	// RETURN = nil | 0
	luaSetValue = newScript(luaDefGetData + luaDefSetData + `
local data = get_data()
if not data then return nil end
if data.value ~= ARGV[1] then
    data.value = ARGV[1]
    set_data(data)
end
return 0`)

	// KEYS   = { KEY }
	// ARGV   = { VALUE, ..., CAPACITY }
	// RETURN = nil | 0
	luaPushMail = newScript(luaDefGetData + luaDefSetData + `
local data = get_data()
if not data then return nil end
for i=1,#ARGV-1,1 do
    data.mailbox.inc = data.mailbox.inc + 1
    local id = string.format("x%08x", data.mailbox.inc)
    data.mailbox.que[#data.mailbox.que+1] = id
    data.mailbox.dict[id] = ARGV[i]
end
local capacity = tonumber(ARGV[#ARGV])
while capacity > 0 and #data.mailbox.que > capacity do
    data.mailbox.dict[data.mailbox.que[1]] = nil
    table.remove(data.mailbox.que, 1)
end
set_data(data)
return 0`)

	// O(min(M*logN, M*logM+M+N))
	// KEYS   = { KEY }
	// ARGV   = { ID ... }
	// RETURN = nil | 0
	luaPullMail = newScript(luaDefGetData + luaDefSetData + `
local data = get_data()
if not data then return nil end
local M, N = #ARGV, #data.mailbox.que
if M  * math.log(N)/math.log(2) < M * math.log(M)/math.log(2) + M + N then
    for _, id in ipairs(ARGV) do
        local min, max = 1, #data.mailbox.que
        while min <= max do
            local mid = math.floor((min+max)/2)
            if data.mailbox.que[mid] < id then
                min = mid + 1
            elseif data.mailbox.que[mid] > id then
                max = mid - 1
            else
                table.remove(data.mailbox.que, mid)
                data.mailbox.dict[id] = nil
                break
            end
        end

    end
else
    table.sort(ARGV)
    local i, j = M, N
    while i > 0 and j > 0 do
        if ARGV[i] > data.mailbox.que[j] then
            i = i - 1
        elseif ARGV[i] < data.mailbox.que[j] then
            j = j - 1
        else
            table.remove(data.mailbox.que, j)
            data.mailbox.dict[ARGV[i]] = nil
            i, j = i - 1, j - 1
        end
    end
end
if #data.mailbox.que ~= N then set_data(data) end
return N - #data.mailbox.que`)

	// KEYS   = { KEY }
	// ARGV   = { SRC }
	// RETURN = { RET, NEW_VALUE }
	luaEvalVarJSON = newScript(luaDefGetData + luaDefSetData + `
local data = get_data()
if not data then return nil end
local var = {}
if #data.value > 0 then var = cjson.decode(data.value) end
local sandbox = assert(loadstring(ARGV[1]))
setfenv(sandbox, var)
local ret = sandbox()
if ret == nil or ret == true then ret = 0 end
if type(ret) ~= "number" then ret = -1 end
if ret == 0 then
    local value = cjson.encode(var)
    if value ~= data.value then
	    data.value = value
	    set_data(data)
	end
end
return { ret, data.value }`)

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
	luaPeekDirty = newScript(luaPeekDirtySrc)

	// clean the first dirty record then get the next
	// KEYS   = { KEY }
	// ARGV   = { VERSION, TTL }
	// RETURN = nil | { KEY, BUF }
	luaNextDirty = newScript(`
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
