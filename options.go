package remon

import (
	"strings"

	log "github.com/ntons/log-go"
)

// map redis key to mongodb (database,collection,_id)
type KeyMappingStrategy interface {
	MapKey(string) (_, _, _ string)
}

type xFuncKeyMappingStrategy struct {
	f func(string) (_, _, _ string)
}

func (f xFuncKeyMappingStrategy) MapKey(key string) (_, _, _ string) {
	return f.f(key)
}

type xDefaultKeyMappingStrategy struct{}

func (xDefaultKeyMappingStrategy) MapKey(key string) (_, _, _ string) {
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

var defaultKeyMappingStrategy = xDefaultKeyMappingStrategy{}

// Client/SyncClient xOptions
type xOptions struct {
	// map redis key to mongodb (database,collection,_id)
	keyMappingStrategy KeyMappingStrategy
	/// for sync only
	// sync limit count per second, 0 means unlimited
	syncRate int
	// logger
	log log.Recorder
}

func newOptions() *xOptions {
	return &xOptions{
		keyMappingStrategy: defaultKeyMappingStrategy,
		log:                log.Nop{},
	}
}

type Option interface {
	apply(o *xOptions)
}

type xFuncOption struct {
	f func(o *xOptions)
}

func (f xFuncOption) apply(o *xOptions) {
	f.f(o)
}

func WithKeyMappingStrategy(v KeyMappingStrategy) Option {
	return xFuncOption{func(o *xOptions) {
		o.keyMappingStrategy = v
	}}
}
func WithKeyMappingStrategyFunc(v func(string) (_, _, _ string)) Option {
	return xFuncOption{func(o *xOptions) {
		o.keyMappingStrategy = xFuncKeyMappingStrategy{v}
	}}
}

func WithSyncRate(rate int) Option {
	return xFuncOption{func(o *xOptions) { o.syncRate = rate }}
}

func WithLogger(logger log.Recorder) Option {
	return xFuncOption{func(o *xOptions) { o.log = logger }}
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
