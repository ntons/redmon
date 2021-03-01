package remon

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/go-redis/redis/v8"
)

var (
	// scripts to load
	scripts []*Script
	// try evalsha probability
	tryEvalShaProb int32
)

func SetTryEvalShaProb(v int32) {
	atomic.StoreInt32(&tryEvalShaProb, v)
}
func shouldTryEvalSha() bool {
	if v := atomic.LoadInt32(&tryEvalShaProb); v > 0 {
		return rand.Int31n(v) == 0
	} else {
		return rand.Int31n(100) == 0
	}
}

// load all scripts to redis
// ReMon can work without invoking LoadScripts or LoadScripts failed
func LoadScripts(ctx context.Context, c RedisClient) (err error) {
	for _, script := range scripts {
		if _, err = script.Load(ctx, c).Result(); err != nil {
			return
		}
	}
	return
}

// Script is almost same to redis.Script
// But evalsha trying strategy in run method
type Script struct {
	src    string
	hash   string
	loaded bool // is script loaded
}

func newScript(src string) *Script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	script := &Script{
		src:    src,
		hash:   hex.EncodeToString(h.Sum(nil)),
		loaded: true,
	}
	scripts = append(scripts, script)
	return script
}

func (script *Script) Hash() string {
	return script.hash
}

func (script *Script) Load(
	ctx context.Context, c RedisClient) *redis.StringCmd {
	return c.ScriptLoad(ctx, script.src)
}

func (script *Script) Exists(
	ctx context.Context, c RedisClient) *redis.BoolSliceCmd {
	return c.ScriptExists(ctx, script.hash)
}

func (script *Script) Eval(
	ctx context.Context, c RedisClient,
	keys []string, args ...interface{}) *redis.Cmd {
	return c.Eval(ctx, script.src, keys, args...)
}

func (script *Script) EvalSha(
	ctx context.Context, c RedisClient,
	keys []string, args ...interface{}) *redis.Cmd {
	return c.EvalSha(ctx, script.hash, keys, args...)
}

func (script *Script) Run(
	ctx context.Context, c RedisClient,
	keys []string, args ...interface{}) *redis.Cmd {
	if !script.loaded {
		script.loaded = shouldTryEvalSha()
	}
	if script.loaded {
		r := script.EvalSha(ctx, c, keys, args...)
		if err := r.Err(); err == nil ||
			!strings.HasPrefix(err.Error(), "NOSCRIPT ") {
			return r
		}
		script.loaded = false
	}
	return script.Eval(ctx, c, keys, args...)
}

const (
	xDirtySet = "$DIRTYSET$"
	xDirtyQue = "$DIRTYQUE$"

	evalScriptTmpl = `
local b=redis.call("GET", KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
local f=assert(loadstring([[%s]]))
local e={}
setmetatable(e,{__index=_G})
e.VALUE=d.val
setfenv(f,e)
local r=f()
if d.val~=e.VALUE then
  d.rev,d.val=d.rev+1,e.VALUE
  redis.call("SET",KEYS[1],cmsgpack.pack(d))
  if redis.call("SADD","` + xDirtySet + `",KEYS[1])>0 then
    redis.call("LPUSH","` + xDirtyQue + `",KEYS[1])
  end
end
return r`
)

func newEvalScript(src string) *Script {
	return newScript(fmt.Sprintf(evalScriptTmpl, src))
}

var (
	// Scripts for ReMon client
	luaGet = newScript(`
local b=redis.call("GET", KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
if d.rev==0 and ARGV[1] then
  d.rev,d.val=d.rev+1,ARGV[1]
  b=cmsgpack.pack(d)
  redis.call("SET",KEYS[1],b)
  if redis.call("SADD","` + xDirtySet + `",KEYS[1])>0 then
    redis.call("LPUSH","` + xDirtyQue + `",KEYS[1])
  end
end
return b
`)

	luaSet = newScript(`
local b=redis.call("GET",KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
d.rev,d.val=d.rev+1,ARGV[1]
redis.call("SET",KEYS[1],cmsgpack.pack(d))
if redis.call("SADD","` + xDirtySet + `",KEYS[1])>0 then
  redis.call("LPUSH","` + xDirtyQue + `",KEYS[1])
end
return d.rev
`)

	luaAdd = newScript(`
local b=redis.call("GET",KEYS[1])
if not b then error("CACHE_MISS") end
local d=cmsgpack.unpack(b)
if d.rev~=0 then return 0 end
d.rev,d.val=d.rev+1,ARGV[1]
redis.call("SET",KEYS[1],cmsgpack.pack(d))
if redis.call("SADD","` + xDirtySet + `",KEYS[1])>0 then
  redis.call("LPUSH","` + xDirtyQue + `",KEYS[1])
end
return 1
`)

	luaLoad = newScript(`
local b=redis.call("GET",KEYS[1])
if not b or cmsgpack.unpack(b).rev<cmsgpack.unpack(ARGV[1]).rev then
  redis.call("SET",KEYS[1],ARGV[1],"EX",86400)
end
return 0
`)

	// Scripts for Sync
	luaPeek = newScript(`
local k=redis.call("LINDEX","` + xDirtyQue + `",-1)
if not k then return end
local b=redis.call("GET",k)
if not b then
  redis.call("RPOP","` + xDirtyQue + `")
  redis.call("SREM","` + xDirtySet + `",k)
  return
end
return {k,b}
`)

	luaNext = newScript(`
if redis.call("LINDEX","` + xDirtyQue + `",-1)==KEYS[1] then
  local b=redis.call("GET",KEYS[1])
  if not b then
    redis.call("RPOP","` + xDirtyQue + `")
    redis.call("SREM","` + xDirtySet + `",KEYS[1])
  else
    local d=cmsgpack.unpack(b)
    if tostring(d.rev)==ARGV[1] then
      redis.call("RPOP","` + xDirtyQue + `")
      redis.call("SREM","` + xDirtySet + `",KEYS[1])
      redis.call("EXPIRE",KEYS[1],86400)
    else
      redis.call("RPOPLPUSH","` + xDirtyQue + `","` + xDirtyQue + `")
    end
  end
end
` + luaPeek.src)

	luaPush = newEvalScript(`
local d={seq=0,que={}}
if #VALUE>0 then d=cmsgpack.unpack(VALUE) end
local cap=tonumber(ARGV[2])
if cap>0 and #d.que>=cap then
  if ARGV[3]==1 then
    while #d.que>=cap do table.remove(d.que,1) end
  else
    return -1
  end
end
d.seq=d.seq+1
d.que[#d.que+1]={id=d.seq,val=ARGV[1]}
VALUE=cmsgpack.pack(d)
return d.seq
`)

	luaPull = newEvalScript(`
local d={seq=0,que={}}
if #VALUE>0 then d=cmsgpack.unpack(VALUE) end
local i,n,r=1,#d.que,{}
for _,id in ipairs(ARGV) do
  local j,v=#d.que,tonumber(id)
  while i<=j do
    local k=math.floor((i+j)/2)
	if d.que[k].id<v then
	  i=k+1
	elseif d.que[k].id>v then
	  j=k-1
	else
	  r[#r+1]=d.que[k].id
	  table.remove(d.que,k)
	  break
	end
  end
end
if #d.que~=n then VALUE=cmsgpack.pack(d) end
return r
`)

	luaClean = newEvalScript(`
local d={seq=0,que={}}
if #VALUE>0 then d=cmsgpack.unpack(VALUE) end
if #d.que>0 then VALUE=cmsgpack.pack({seq=d.seq,que={}}) end
return #d.que
`)
)
