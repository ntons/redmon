package mailbox

import (
	"context"
	"fmt"
	"sort"

	remon "github.com/ntons/remon-go"
	"github.com/vmihailenco/msgpack/v4"
)

type Mail struct {
	Id      string `msgpack:"id"`
	Content string `msgpack:"content"`
}

func (m Mail) String() string {
	return fmt.Sprintf("%s:%s", m.Id, m.Content)
}

// archive structure
type data struct {
	Seq int64   `msgpack:"seq"` // id generater
	Que []*Mail `msgpack:"que"` // mail queue, ordered by id
}

// mailbox client
type Client struct {
	client *remon.Client
	opts   []Option
}

func New(client *remon.Client, opts ...Option) Client {
	return Client{client: client, opts: opts}
}

// list mails in box
func (c Client) List(ctx context.Context, key string) (_ []*Mail, err error) {
	val, err := c.client.Get(ctx, key)
	if err != nil {
		return
	}
	var d data
	if err = msgpack.Unmarshal(s2b(val), &d); err != nil {
		return
	}
	return d.Que, nil
}

// push mail to box
var luaPush = remon.NewEvalScript(`local d={seq=0,que={}} if #VALUE>0 then d=cmsgpack.unpack(VALUE) end d.seq=d.seq+1 local id=string.format("%08X",d.seq) d.que[#d.que+1]={id=id,content=ARGV[1]} if ARGV[2] then local cap=tonumber(ARGV[2]) while cap>0 and #d.que>cap do table.remove(d.que,1) end end VALUE=cmsgpack.pack(d) return id`)

func (c Client) Push(
	ctx context.Context, key, content string, opts ...Option) (
	id string, err error) {
	var o options
	for _, opt := range append(c.opts, opts...) {
		opt.apply(&o)
	}
	if id, err = c.client.Eval(
		ctx, luaPush, key, content, o.capacity).Text(); err != nil {
		return
	}
	return
}

// pull mail(s) from box
// ARGV must be sorted
var luaPull = remon.NewEvalScript(`local d={seq=0,que={}} if #VALUE>0 then d=cmsgpack.unpack(VALUE) end local i,n=1,#d.que for _, id in ipairs(ARGV) do local j=#d.que while i<=j do local k=math.floor((i+j)/2) if d.que[k].id < id then i = k + 1 elseif d.que[k].id > id then j = k - 1 else table.remove(d.que, k) break end end end if #d.que ~= n then VALUE=cmsgpack.pack(d) end return n-#d.que`)

func (c Client) Pull(
	ctx context.Context, key string, ids ...string) (err error) {
	if len(ids) == 0 {
		return
	}
	sort.Strings(ids)
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	return c.client.Eval(ctx, luaPull, key, args...).Err()
}

var luaDrain = remon.NewEvalScript(`local d={seq=0,que={}} if #VALUE>0 then d=cmsgpack.unpack(VALUE) end if #d.que>0 then VALUE=cmsgpack.pack({seq=d.seq,que={}}) end return #d.que`)

func (c Client) Drain(ctx context.Context, key string) (err error) {
	return c.client.Eval(ctx, luaDrain, key).Err()
}
