package remon

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

type RedisClient interface {
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

type xScript struct {
	src, hash string
	// mutex for loading
	mu sync.Mutex
}

func newScript(src string) *xScript {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	return &xScript{src: src, hash: hex.EncodeToString(h.Sum(nil))}
}

func newScriptFormat(pat string, args ...interface{}) *xScript {
	return newScript(fmt.Sprintf(pat, args...))
}

func newScriptReplace(src string, oldnew ...string) *xScript {
	return newScript(strings.NewReplacer(oldnew...).Replace(src))
}

func (script *xScript) Run(ctx context.Context, cli RedisClient, keys []string, args ...interface{}) (r *redis.Cmd) {
	if r = cli.EvalSha(ctx, script.hash, keys, args...); !isNoScript(r.Err()) {
		return
	}
	script.mu.Lock()
	if r = cli.EvalSha(ctx, script.hash, keys, args...); !isNoScript(r.Err()) {
		script.mu.Unlock()
		return
	}
	if err := cli.ScriptLoad(ctx, script.src).Err(); err != nil {
		script.mu.Unlock()
		r = redis.NewCmd(ctx)
		r.SetErr(err)
		return
	}
	script.mu.Unlock()
	return cli.EvalSha(ctx, script.hash, keys, args...)
}
