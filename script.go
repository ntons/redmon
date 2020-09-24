package remon

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"math/rand"
	"strings"

	"github.com/go-redis/redis/v7"
)

// scripts create by remon.NewScript
var scripts []*Script

// load all scripts to redis
// NewScript must be invoked before LoadScripts
func LoadScripts(c redisClient) (err error) {
	for _, s := range scripts {
		if _, err = s.Load(c).Result(); err != nil {
			return
		}
	}
	return
}

type ScriptOption interface {
	apply(*Script)
}

type funcScriptOption struct {
	fn func(*Script)
}

func (o funcScriptOption) apply(s *Script) {
	o.fn(s)
}

func WithTryShaProb(prob int) ScriptOption {
	return funcScriptOption{func(s *Script) { s.tryShaProb = prob }}
}

// Script is similar to redis.Script, but optimize evalsha strategy
type Script struct {
	src        string
	hash       string
	loaded     bool // is script loaded
	tryShaProb int  // trySha probability 1/n
}

func NewScript(src string, opts ...ScriptOption) *Script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	s := &Script{
		src:        src,
		hash:       hex.EncodeToString(h.Sum(nil)),
		loaded:     true,
		tryShaProb: 100, // 1% by default
	}
	for _, opt := range opts {
		opt.apply(s)
	}
	scripts = append(scripts, s)
	return s
}

func (s *Script) Hash() string {
	return s.hash
}

func (s *Script) Load(c redisClient) *redis.StringCmd {
	return c.ScriptLoad(s.src)
}

func (s *Script) Exists(c redisClient) *redis.BoolSliceCmd {
	return c.ScriptExists(s.hash)
}

func (s *Script) Eval(c redisClient, keys []string, args ...interface{}) *redis.Cmd {
	return c.Eval(s.src, keys, args...)
}

func (s *Script) EvalContext(ctx context.Context, c redisClient, keys []string, args ...interface{}) *redis.Cmd {
	cmdArgs := make([]interface{}, 3+len(keys), 3+len(keys)+len(args))
	cmdArgs[0] = "eval"
	cmdArgs[1] = s.src
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	cmdArgs = append(cmdArgs, args...)
	cmd := redis.NewCmd(cmdArgs...)
	c.ProcessContext(ctx, cmd)
	return cmd
}

func (s *Script) EvalSha(c redisClient, keys []string, args ...interface{}) *redis.Cmd {
	return c.EvalSha(s.hash, keys, args...)
}

func (s *Script) EvalShaContext(ctx context.Context, c redisClient, keys []string, args ...interface{}) *redis.Cmd {
	cmdArgs := make([]interface{}, 3+len(keys), 3+len(keys)+len(args))
	cmdArgs[0] = "evalsha"
	cmdArgs[1] = s.hash
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	cmdArgs = append(cmdArgs, args...)
	cmd := redis.NewCmd(cmdArgs...)
	c.ProcessContext(ctx, cmd)
	return cmd
}

func (s *Script) Run(c redisClient, keys []string, args ...interface{}) *redis.Cmd {
	if !s.loaded {
		s.loaded = s.trySha()
	}
	if s.loaded {
		r := s.EvalSha(c, keys, args...)
		if err := r.Err(); err == nil || !strings.HasPrefix(err.Error(), "NOSCRIPT ") {
			return r
		}
		s.loaded = false
	}
	return s.Eval(c, keys, args...)
}

func (s *Script) RunContext(ctx context.Context, c redisClient, keys []string, args ...interface{}) *redis.Cmd {
	if !s.loaded {
		s.loaded = s.trySha()
	}
	if s.loaded {
		r := s.EvalShaContext(ctx, c, keys, args...)
		if err := r.Err(); err == nil || !strings.HasPrefix(err.Error(), "NOSCRIPT ") {
			return r
		}
		s.loaded = false
	}
	return s.EvalContext(ctx, c, keys, args...)
}

func (s *Script) trySha() bool {
	return rand.Intn(100) == 0
}
