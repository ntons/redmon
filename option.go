package remon

import (
	"strings"

	log "github.com/ntons/log-go"
)

// map redis key to mongodb (database,collection,_id)
type KeyMappingStrategy interface {
	MapKey(string) (_, _, _ string)
}

type funcKeyMappingStrategy struct {
	f func(string) (_, _, _ string)
}

func (f funcKeyMappingStrategy) MapKey(key string) (_, _, _ string) {
	return f.f(key)
}

type defaultKeyMappingStrategy struct {
}

func (defaultKeyMappingStrategy) MapKey(key string) (_, _, _ string) {
	a := strings.SplitN(key, ":", 3)
	switch len(a) {
	case 3:
		return a[0], a[1], a[2]
	case 2:
		return "remon", a[0], a[1]
	default:
		return "remon", "default", key
	}
}

// ReMon/Sync options
type options struct {
	// map redis key to mongodb (database,collection,_id)
	keyMappingStrategy KeyMappingStrategy
	/// for sync only
	// sync limit count per second, 0 means unlimited
	rate int
	// logger
	log log.Recorder
}

func newOptions() *options {
	return &options{
		keyMappingStrategy: defaultKeyMappingStrategy{},
		log:                log.Nop{},
	}
}

type Option interface {
	apply(o *options)
}

type funcOption struct {
	f func(o *options)
}

func (f funcOption) apply(o *options) {
	f.f(o)
}

func WithKeyMappingStrategy(v KeyMappingStrategy) Option {
	return funcOption{func(o *options) {
		o.keyMappingStrategy = v
	}}
}
func WithKeyMappingStrategyFunc(v func(string) (_, _, _ string)) Option {
	return funcOption{func(o *options) {
		o.keyMappingStrategy = funcKeyMappingStrategy{v}
	}}
}

func WithSyncRate(rate int) Option {
	return funcOption{func(o *options) { o.rate = rate }}
}

func WithLogger(logger log.Recorder) Option {
	return funcOption{func(o *options) { o.log = logger }}
}

// add options
type getOptions struct {
	addIfNotFound *string
}

type GetOption interface {
	apply(o *getOptions)
}

type funcGetOption struct {
	f func(o *getOptions)
}

func (f funcGetOption) apply(o *getOptions) {
	f.f(o)
}

func AddIfNotFound(val string) GetOption {
	return funcGetOption{func(o *getOptions) { o.addIfNotFound = &val }}
}
