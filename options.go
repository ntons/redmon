package remon

import (
	"strings"
	"time"
)

type KeyMapper func(string) (_, _, _ string)

type OnSyncSaveFunc func(string) time.Duration
type OnSyncErrorFunc func(error) time.Duration
type OnSyncIdleFunc func() time.Duration

// Client/SyncClient xOptions
type xOptions struct {
	keyMapper   KeyMapper
	onSyncSave  OnSyncSaveFunc
	onSyncError OnSyncErrorFunc
	onSyncIdle  OnSyncIdleFunc
}

func (x *xOptions) MapKey(key string) (_, _, _ string) {
	if x.keyMapper != nil {
		return x.keyMapper(key)
	}
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
func (x *xOptions) OnSyncSave(key string) time.Duration {
	if x.onSyncSave != nil {
		return x.onSyncSave(key)
	}
	return 0
}
func (x *xOptions) OnSyncError(err error) time.Duration {
	if x.onSyncError != nil {
		return x.onSyncError(err)
	}
	return time.Second
}
func (x xOptions) OnSyncIdle() time.Duration {
	if x.onSyncIdle != nil {
		return x.onSyncIdle()
	}
	return time.Second
}

type Option interface {
	apply(o *xOptions)
}

type xFuncOption struct {
	fn func(o *xOptions)
}

func (x xFuncOption) apply(o *xOptions) {
	x.fn(o)
}

func WithKeyMapper(fn KeyMapper) Option {
	return xFuncOption{func(o *xOptions) { o.keyMapper = fn }}
}
func OnSyncSave(fn OnSyncSaveFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncSave = fn }}
}
func OnSyncError(fn OnSyncErrorFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncError = fn }}
}
func OnSyncIdle(fn OnSyncIdleFunc) Option {
	return xFuncOption{func(o *xOptions) { o.onSyncIdle = fn }}
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
