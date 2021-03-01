package remon

import (
	"context"
	"sort"

	"github.com/vmihailenco/msgpack/v4"
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

func NewMailClient(rm Client) *xMailClient {
	return &xMailClient{rm: rm}
}

func (cli xMailClient) List(
	ctx context.Context, key string) (_ []*Mail, err error) {
	_, val, err := cli.rm.Get(ctx, key)
	if err != nil {
		return
	}
	var data xMailData
	if err = msgpack.Unmarshal(fastStringToBytes(val), &data); err != nil {
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
	if id, err = cli.rm.Eval(
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
	sort.Sort(Int64Slice(ids)) // ARGV must be sorted
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	r, err := cli.rm.Eval(ctx, luaPull, key, args...).Result()
	if err != nil {
		return
	}
	for _, v := range r.([]interface{}) {
		pulled = append(pulled, v.(int64))
	}
	return
}

func (cli xMailClient) Clean(ctx context.Context, key string) (err error) {
	return cli.rm.Eval(ctx, luaClean, key).Err()
}
