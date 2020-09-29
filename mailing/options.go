package mailing

type options struct {
	capacity int32
}

type Option interface {
	apply(o *options)
}

type funcOption struct {
	fn func(o *options)
}

func (f funcOption) apply(o *options) {
	f.fn(o)
}

func WithCapacity(capacity int32) Option {
	return funcOption{func(o *options) { o.capacity = capacity }}
}
