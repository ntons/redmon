package remon

import (
	"strings"
	"time"
)

// Redis(key) -> Mongo(db,collection,_id)
type MapKeyFunc func(key string) (db, collection, _id string)

// 同步成功回调函数
type OnSyncSaveFunc func(key string) time.Duration

// 同步失败回调函数
type OnSyncErrorFunc func(err error) time.Duration

// 同步空闲回调函数
type OnSyncIdleFunc func() time.Duration

// Client/SyncClient xOptions
type xOptions struct {
	mapKeyFunc      MapKeyFunc
	onSyncSaveFunc  OnSyncSaveFunc
	onSyncErrorFunc OnSyncErrorFunc
	onSyncIdleFunc  OnSyncIdleFunc
}

func (x *xOptions) mapKey(key string) (_, _, _ string) {
	if x.mapKeyFunc != nil {
		return x.mapKeyFunc(key)
	}
	// default policy
	a := strings.SplitN(key, ":", 3)
	switch len(a) {
	case 3:
		return a[0], a[1], a[2]
	case 2:
		return "remon", a[0], a[1]
	default:
		return "remon", "data", key
	}
}
func (x *xOptions) onSyncSave(key string) time.Duration {
	if x.onSyncSaveFunc != nil {
		return x.onSyncSaveFunc(key)
	}
	return 0
}
func (x *xOptions) onSyncError(err error) time.Duration {
	if x.onSyncErrorFunc != nil {
		return x.onSyncErrorFunc(err)
	}
	return time.Second
}
func (x xOptions) onSyncIdle() time.Duration {
	if x.onSyncIdleFunc != nil {
		return x.onSyncIdleFunc()
	}
	return time.Second
}

type Option interface {
	apply(o *xOptions)
}

type xFuncOption struct {
	f func(o *xOptions)
}

func (x xFuncOption) apply(o *xOptions) { x.f(o) }

func WithKeyMap(f MapKeyFunc) Option {
	return xFuncOption{func(o *xOptions) { o.mapKeyFunc = f }}
}
func OnSyncSave(f OnSyncSaveFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncSaveFunc = f }}
}
func OnSyncError(f OnSyncErrorFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncErrorFunc = f }}
}
func OnSyncIdle(f OnSyncIdleFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncIdleFunc = f }}
}

// get method xOptions
type GetOption interface {
	apply(o *xGetOptions)
}
type xGetOptions struct {
	// add or set atomically
	addIfNotFound *string
}
type xFuncGetOption struct {
	f func(o *xGetOptions)
}

func (f xFuncGetOption) apply(o *xGetOptions) { f.f(o) }

func AddIfNotFound(v string) GetOption {
	return xFuncGetOption{func(o *xGetOptions) { o.addIfNotFound = &v }}
}

// push method options
type PushStrategy int

const (
	RejectOnFull     PushStrategy = 0
	PullOldestOnFull PushStrategy = 1
)

type PushOption interface {
	apply(o *xPushOptions)
}
type xPushOptions struct {
	// capacity of set
	capacity int
	// full strategy
	strategy PushStrategy
}
type xFuncPushOption struct {
	f func(o *xPushOptions)
}

func (f xFuncPushOption) apply(o *xPushOptions) { f.f(o) }

func WithCapacity(v int) PushOption {
	return xFuncPushOption{func(o *xPushOptions) { o.capacity = v }}
}
func WithPushStrategy(v PushStrategy) PushOption {
	return xFuncPushOption{func(o *xPushOptions) { o.strategy = v }}
}
