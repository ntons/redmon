package remon

import (
	"context"
	"fmt"
	"sort"

	"github.com/vmihailenco/msgpack/v4"
)

type Element struct {
	// auto-generated id to identify a element
	Id string `msgpack:"id"`
	// element payload
	Val string `msgpack:"val"`
}

func (e Element) String() string {
	return fmt.Sprintf("%s:%s", e.Id, e.Val)
}

// archive structure
type xSetData struct {
	Seq int64      `msgpack:"seq"` // id generater
	Que []*Element `msgpack:"que"` // queue, ordered by id
}

type SetClient interface {
	List(ctx context.Context, key string) (_ []*Element, err error)
	Push(ctx context.Context, key, val string, capacity int32) (id string, err error)
	Pull(ctx context.Context, key string, ids ...string) (pulled []string, err error)
	Clean(ctx context.Context, key string) (err error)
}

var _ SetClient = (*xSetClient)(nil)

// set client
type xSetClient struct {
	rm Client
}

func NewSetClient(rm Client) *xSetClient {
	return &xSetClient{rm: rm}
}

// list elements in set
func (cli xSetClient) List(
	ctx context.Context, key string) (_ []*Element, err error) {
	_, val, err := cli.rm.Get(ctx, key)
	if err != nil {
		return
	}
	var data xSetData
	if err = msgpack.Unmarshal(s2b(val), &data); err != nil {
		return
	}
	return data.Que, nil
}

// push element to set
func (cli xSetClient) Push(
	ctx context.Context, key, val string, capacity int32) (
	id string, err error) {
	if id, err = cli.rm.Eval(
		ctx, luaPush, key, val, capacity).Text(); err != nil {
		return
	}
	return
}

// pull elements from set
func (cli xSetClient) Pull(
	ctx context.Context, key string, ids ...string) (
	pulled []string, err error) {
	if len(ids) == 0 {
		return
	}
	sort.Strings(ids) // ARGV must be sorted
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	r, err := cli.rm.Eval(ctx, luaPull, key, args...).Result()
	if err != nil {
		return
	}
	for _, v := range r.([]interface{}) {
		pulled = append(pulled, v.(string))
	}
	return
}

// remove all elements from set
func (cli xSetClient) Clean(ctx context.Context, key string) (err error) {
	return cli.rm.Eval(ctx, luaDrain, key).Err()
}
