package remon

import (
	"context"
	"sort"

	"github.com/vmihailenco/msgpack/v4"
)

func newSandboxScript(src string) *xScript {
	const tmpl = `
local b = redis.call("GET", KEYS[1])
if not b then return end
local d = cmsgpack.unpack(b)
local f = assert(loadstring([[%s]]))
local e = {}
setmetatable(e, {__index=_G})
e.VALUE = d.val
setfenv(f, e)
local r = f()
if d.val ~= e.VALUE then
  d.rev, d.val = d.rev+1, e.VALUE
  redis.call("SET", KEYS[1], cmsgpack.pack(d))
  if redis.call("SADD", "%s", KEYS[1]) > 0 then redis.call("LPUSH", "%s", KEYS[1]) end
end
return r
`
	return newScriptFormat(tmpl, src, xDirtySet, xDirtyQue)
}

var (
	luaPush = newSandboxScript(`
local d= {seq=0, que={}}
if #VALUE > 0 then d = cmsgpack.unpack(VALUE) end
local cap = tonumber(ARGV[2])
if cap > 0 and #d.que >= cap then
  if ARGV[3] == 1 then
    while #d.que >= cap do table.remove(d.que, 1) end
  else
    return -1
  end
end
d.seq = d.seq+1
d.que[#d.que+1] = {id=d.seq, val=ARGV[1]}
VALUE = cmsgpack.pack(d)
return d.seq
`)

	luaPull = newSandboxScript(`
local d = {seq=0, que={}}
if #VALUE > 0 then d = cmsgpack.unpack(VALUE) end
local i, n, r = 1, #d.que, {}
for _, id in ipairs(ARGV) do
  local j, v = #d.que, tonumber(id)
  while i <= j do
    local k = math.floor((i+j)/2)
	if d.que[k].id < v then
	  i = k+1
	elseif d.que[k].id > v then
	  j = k-1
	else
	  r[#r+1] = d.que[k].id
	  table.remove(d.que, k)
	  break
	end
  end
end
if #d.que ~= n then VALUE = cmsgpack.pack(d) end
return r
`)

	luaClean = newSandboxScript(`
local d = {seq=0, que={}}
if #VALUE > 0 then d = cmsgpack.unpack(VALUE) end
if #d.que > 0 then VALUE = cmsgpack.pack({seq=d.seq, que={}}) end
return #d.que
`)
)

type Mail struct {
	// auto-generated seq to identify a mail
	Id int64 `msgpack:"id"`
	// mail payload
	Val string `msgpack:"val"`
}

type MailClient interface {
	// list mails
	List(ctx context.Context, key string) (_ []*Mail, err error)
	// push mail in
	Push(ctx context.Context, key, val string, opts ...PushOption) (id int64, err error)
	// pull mail(s) out
	Pull(ctx context.Context, key string, ids ...int64) (pulled []int64, err error)
	// clean all mails
	Clean(ctx context.Context, key string) (err error)
}

// archive structure
type xMailData struct {
	Seq int64   `msgpack:"seq"` // id generater
	Que []*Mail `msgpack:"que"` // queue, ordered by id
}

var _ MailClient = (*xMailClient)(nil)

type xMailClient struct{ rm Client }

func NewMailClient(rm Client) MailClient {
	return &xMailClient{rm: rm}
}

func (cli xMailClient) List(
	ctx context.Context, key string) (_ []*Mail, err error) {
	_, val, err := cli.rm.Get(ctx, key)
	if err != nil {
		return
	}
	var data xMailData
	if err = msgpack.Unmarshal(s2b(val), &data); err != nil {
		return
	}
	return data.Que, nil
}

func (cli xMailClient) Push(
	ctx context.Context, key, val string, opts ...PushOption) (
	id int64, err error) {
	var o xPushOptions
	for _, opt := range opts {
		opt.apply(&o)
	}
	if id, err = cli.rm.eval(
		ctx, luaPush, key, val, o.capacity, int(o.strategy),
	).Int64(); err != nil {
		return
	}
	if id == -1 {
		return 0, ErrMailFull
	}
	return
}

func (cli xMailClient) Pull(
	ctx context.Context, key string, ids ...int64) (pulled []int64, err error) {
	if len(ids) == 0 {
		return
	}
	sort.Sort(int64Slice(ids)) // ARGV must be sorted
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	r, err := cli.rm.eval(ctx, luaPull, key, args...).Result()
	if err != nil {
		return
	}
	for _, v := range r.([]interface{}) {
		pulled = append(pulled, v.(int64))
	}
	return
}

func (cli xMailClient) Clean(ctx context.Context, key string) (err error) {
	return cli.rm.eval(ctx, luaClean, key).Err()
}
