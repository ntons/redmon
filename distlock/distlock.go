package redislock

/*
import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
)

type RedisClient interface { // redis.scripter
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(script string) *redis.StringCmd
}

func randToken(t time.Time) string {
	const alphabet = "0123456789abcdef"
	sb := strings.Builder{}
	for i := 0; i < 8; i++ {
		sb.WriteByte(alphabet[rand.Intn(len(alphabet))])
	}
	sb.WriteString(fmt.Sprintf("%x", t.UnixNano()))
	return sb.String()
}

func getTokenTime(token string) time.Time {
	nano, err := strconv.ParseInt(token[8:], 16, 64)
	if err != nil {
		panic(err)
	}
	return time.Unix(0, nano)
}

func ms(v int64) time.Duration {
	return time.Duration(v) * time.Millisecond
}

func Lock(r RedisClient, key string, expire time.Time, opts ...LockOption) (_ string, err error) {
	var o = lockOptions{
		retry:      noRetry{},
		retrycount: nil,
		ctx:        context.Background(),
	}
	for _, opt := range opts {
		opt.apply(&o)
	}
	for i := 0; ; i++ {
		if o.retrycount == nil {
			*o.retrycount = i
		}

		now := time.Now()

		ttl := int64(expire.Sub(now) / time.Millisecond)
		if ttl < 1 {
			err = ErrNotLocked
			return
		}

		token := randToken(now)
		var v interface{}
		if v, err = luaLock.EvalSha(
			r, []string{key}, token, ttl).Result(); err != nil {
			return
		}

		a, ok := v.([]interface{})
		if !ok || len(a) != 2 {
			err = fmt.Errorf("redislock: malformed lua return type %T", v)
			return
		}

		a0, ok := a[0].(string)
		if !ok {
			err = fmt.Errorf("redislock: malformed lua return type %T", a[0])
			return
		}
		if a0 == token {
			return token, nil // success
		}

		a1, ok := a[1].(int64)
		if !ok {
			err = fmt.Errorf("redislock: malformed lua return type %T", a[1])
			return
		}

		backoff := o.retry.GetBackoff(getTokenTime(a0), ms(a1))
		if backoff < 1 {
			err = ErrNotLocked
			return
		}

		select {
		case <-o.ctx.Done():
			return "", o.ctx.Err()
		case <-time.After(backoff):
		}
	}
}

func Unlock(r RedisClient, key, token string, opts ...UnlockOption) (err error) {
	var o = unlockOptions{}
	for _, opt := range opts {
		opt.apply(&o)
	}
	if token == "" {
		return ErrLockNotHeld
	}
	v, err := luaUnlock.EvalSha(r, []string{key}, token).Int64()
	if err != nil {
		return
	}
	if o.elapse != nil {
		*o.elapse = time.Since(getTokenTime(token))
	}
	return luaRetToErr(v)
}

func Verify(r RedisClient, key, token string) (err error) {
	v, err := luaCheck.EvalSha(r, []string{key}, token).Int64()
	if err != nil {
		return
	}
	if v < 0 {
		return luaRetToErr(v)
	}
	return
}

func Refresh(r RedisClient, key, token string, expire time.Time) (err error) {
	ttl := int64(expire.Sub(time.Now()) / time.Millisecond)
	v, err := luaRefresh.EvalSha(r, []string{key}, token, ttl).Int64()
	if err != nil {
		return
	}
	return luaRetToErr(v)
}
*/
