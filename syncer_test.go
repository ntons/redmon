package remon

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v4"
)

func TestSyncerPeekNext(t *testing.T) {
	r, m := dial(t)
	s := NewSyncer(r, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"
	r.Del(ctx, xDirtyQue, xDirtySet, key)

	if _, _, err := s.peek(); err != redis.Nil {
		t.Fatalf("unexpected peek error: %v", err)
	}

	b, _ := msgpack.Marshal(xRedisData{Rev: 1, Val: val})
	r.Set(ctx, key, fastBytesToString(b), 0)
	r.SAdd(ctx, xDirtySet, key)
	r.LPush(ctx, xDirtyQue, key)

	if k, d, err := s.peek(); err != nil {
		t.Fatalf("unexpected peek error: %v", err)
	} else if k != key {
		t.Fatalf("unexpected peek key: %v", k)
	} else if d.Rev != 1 {
		t.Fatalf("unexpected peek rev: %v", d.Rev)
	} else if d.Val != val {
		t.Fatalf("unexpected peek val: %v", d.Val)
	}

	if k, d, err := s.next(key, 2); err != nil {
		t.Fatalf("unexpected peek error: %v", err)
	} else if k != key {
		t.Fatalf("unexpected peek key: %v", k)
	} else if d.Rev != 1 {
		t.Fatalf("unexpected peek rev: %v", d.Rev)
	} else if d.Val != val {
		t.Fatalf("unexpected peek val: %v", d.Val)
	}

	if _, _, err := s.next(key, 1); err != redis.Nil {
		t.Fatalf("unexpected peek error: %v", err)
	}
}
