package redmon

import (
	"strings"
	"time"
)

// Redis(key) -> Mongo(db,collection,_id)
type KeyMappingFunc func(key string) (db, collection, _id string)

// 同步成功回调函数
type OnSyncSaveFunc func(key string) time.Duration

// 同步失败回调函数
type OnSyncFailFunc func(err error) time.Duration

// 同步空闲回调函数
type OnSyncIdleFunc func() time.Duration

// Client Options
type (
	xOptions struct {
		keyMappingFunc KeyMappingFunc
		onSyncSaveFunc OnSyncSaveFunc
		onSyncFailFunc OnSyncFailFunc
		onSyncIdleFunc OnSyncIdleFunc
	}
	xFuncOption struct {
		f func(o *xOptions)
	}
	Option interface {
		apply(o *xOptions)
	}
)

func (x *xOptions) mapKey(key string) (_, _, _ string) {
	if x.keyMappingFunc != nil {
		return x.keyMappingFunc(key)
	}
	// default policy
	a := strings.SplitN(key, ":", 3)
	switch len(a) {
	case 3:
		return a[0], a[1], a[2]
	case 2:
		return "redmon", a[0], a[1]
	default:
		return "redmon", "data", key
	}
}

func (x *xOptions) onSyncSave(key string) time.Duration {
	if x.onSyncSaveFunc != nil {
		return x.onSyncSaveFunc(key)
	}
	return 0
}

func (x *xOptions) onSyncFail(err error) time.Duration {
	if x.onSyncFailFunc != nil {
		return x.onSyncFailFunc(err)
	}
	return time.Second
}

func (x xOptions) onSyncIdle() time.Duration {
	if x.onSyncIdleFunc != nil {
		return x.onSyncIdleFunc()
	}
	return time.Second
}

func (x xFuncOption) apply(o *xOptions) { x.f(o) }

func WithKeyMap(f KeyMappingFunc) Option {
	return xFuncOption{func(o *xOptions) { o.keyMappingFunc = f }}
}
func OnSyncSave(f OnSyncSaveFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncSaveFunc = f }}
}
func OnSyncFail(f OnSyncFailFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncFailFunc = f }}
}
func OnSyncIdle(f OnSyncIdleFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncIdleFunc = f }}
}

// Client.Get Options
type (
	xGetOptions struct {
		// add or set atomically
		addIfNotExists *string
	}
	xGetOptionFunc struct {
		f func(o *xGetOptions)
	}
	GetOption interface {
		apply(o *xGetOptions)
	}
)

func (f xGetOptionFunc) apply(o *xGetOptions) { f.f(o) }

func AddIfNotExists(v string) GetOption {
	return xGetOptionFunc{func(o *xGetOptions) { o.addIfNotExists = &v }}
}

// Client.Push Options
type (
	xPushOptions struct {
		// importance [0,255]
		importance uint8
		// capacity of set [1,65535]
		capacity uint16
		// strategy on full
		strategy int
	}
	xPushOptionFunc struct {
		f func(o *xPushOptions)
	}
	PushOption interface {
		apply(o *xPushOptions)
	}
)

func (f xPushOptionFunc) apply(o *xPushOptions) { f.f(o) }

func WithImportance(v uint8) PushOption {
	return xPushOptionFunc{func(o *xPushOptions) { o.importance = v }}
}
func WithCapacity(v uint16) PushOption {
	return xPushOptionFunc{func(o *xPushOptions) { o.capacity = v }}
}
func WithRing() PushOption {
	return xPushOptionFunc{func(o *xPushOptions) { o.strategy = 1 }}
}
