package redislock

import (
	"context"
	"time"
)

type BackoffFunc = func(time.Time, time.Duration) time.Duration

type RetryStrategy interface {
	GetBackoff(time.Time, time.Duration) time.Duration
}

type noRetry struct {
}

func (noRetry) GetBackoff(time.Time, time.Duration) time.Duration { return 0 }

type funcRetryStrategy struct {
	f BackoffFunc
}

func (f funcRetryStrategy) GetBackoff(
	t time.Time, d time.Duration) time.Duration {
	return f.f(t, d)
}

type lockOptions struct {
	// retry strategy
	retry RetryStrategy
	// retrycount
	retrycount *int
	// context
	ctx context.Context
}

type LockOption interface {
	apply(o *lockOptions)
}

type funcLockOption struct {
	f func(*lockOptions)
}

func (f funcLockOption) apply(o *lockOptions) {
	f.f(o)
}

func WithContext(ctx context.Context) LockOption {
	return funcLockOption{func(o *lockOptions) { o.ctx = ctx }}
}

func WithRetry(retry RetryStrategy) LockOption {
	return funcLockOption{func(o *lockOptions) { o.retry = retry }}
}

func WithBackoffFunc(f BackoffFunc) LockOption {
	return funcLockOption{func(o *lockOptions) {
		o.retry = funcRetryStrategy{f}
	}}
}

func WithBackoff(backoff time.Duration) LockOption {
	return funcLockOption{func(o *lockOptions) {
		o.retry = funcRetryStrategy{
			func(time.Time, time.Duration) time.Duration {
				return backoff
			}}
	}}
}

func WithRetryCount(v *int) LockOption {
	return funcLockOption{func(o *lockOptions) { o.retrycount = v }}
}

type unlockOptions struct {
	elapse *time.Duration
}

type UnlockOption interface {
	apply(o *unlockOptions)
}

type funcUnlockOption struct {
	f func(*unlockOptions)
}

func (f funcUnlockOption) apply(o *unlockOptions) {
	f.f(o)
}

func WithElapse(v *time.Duration) UnlockOption {
	return funcUnlockOption{func(o *unlockOptions) { o.elapse = v }}
}
