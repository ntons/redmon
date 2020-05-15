package remon

import (
	"context"

	"github.com/go-redis/redis/v7"
	"github.com/ntons/tongo/tunsafe"
)

func (x *ReMon) ListMail(
	ctx context.Context, key string) (list []Mail, err error) {
	if list, err = x.getMailListFromRedis(ctx, key); err != nil {
		if err != redis.Nil {
			return
		}
		var data _Data
		if data, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		list = data.getMailList()
	}
	return
}

func (x *ReMon) ListMailBytes(
	ctx context.Context, key string) (list []MailBytes, err error) {
	_list, err := x.ListMail(ctx, key)
	if err != nil {
		return
	}
	list = make([]MailBytes, len(_list))
	for i, m := range _list {
		list[i].Id = m.Id
		list[i].Value = tunsafe.StringToBytes(m.Value)
	}
	return
}

func (x *ReMon) PushMail(
	ctx context.Context, key string, values []string, opts ...PushOption) (
	err error) {
	var o = &pushOptions{}
	for _, opt := range opts {
		opt.apply(o)
	}
	if err = x.pushMailToRedis(ctx, key, values, o); err != nil {
		if err != redis.Nil {
			return
		}
		if _, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			if err != ErrNotExist {
				return
			}
			if !o.addOnNotExist {
				return
			}
		}
		if err = x.pushMailToRedis(ctx, key, values, o); err != nil {
			return
		}
	}
	return
}
func (x *ReMon) PushMailBytes(
	ctx context.Context, key string, values [][]byte, opts ...PushOption) (
	err error) {
	_values := make([]string, len(values))
	for i, value := range values {
		_values[i] = tunsafe.BytesToString(value)
	}
	return x.PushMail(ctx, key, _values, opts...)
}

func (x *ReMon) PushOneMail(
	ctx context.Context, key, value string, opts ...PushOption) (err error) {
	return x.PushMail(ctx, key, []string{value}, opts...)
}
func (x *ReMon) PushOneMailBytes(
	ctx context.Context, key string, value []byte, opts ...PushOption) (
	err error) {
	return x.PushMailBytes(ctx, key, [][]byte{value}, opts...)
}

func (x *ReMon) PullMail(
	ctx context.Context, key string, ids []string) (n int, err error) {
	if n, err = x.pullMailFromRedis(ctx, key, ids); err != nil {
		if err != redis.Nil {
			return
		}
		if _, err = x.loadDataFromMongoToRedis(ctx, key); err != nil {
			return
		}
		if n, err = x.pullMailFromRedis(ctx, key, ids); err != nil {
			return
		}
	}
	return
}

func (x *ReMon) getMailListFromRedis(
	ctx context.Context, key string) (list []Mail, err error) {
	data, err := x.getDataFromRedis(ctx, key)
	if err != nil {
		return
	}
	if data.Version == 0 {
		err = ErrNotExist
		return
	}
	list = data.getMailList()
	return
}
func (x *ReMon) pushMailToRedis(
	ctx context.Context, key string, values []string, o *pushOptions) (err error) {
	var args = make([]interface{}, 0, len(values)+1)
	for _, value := range values {
		args = append(args, value)
	}
	args = append(args, o.capacity)
	return x.eval(ctx, luaPushMail, key, args...).Err()
}
func (x *ReMon) pullMailFromRedis(
	ctx context.Context, key string, ids []string) (n int, err error) {
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return x.eval(ctx, luaPullMail, key, args...).Int()
}
