package remon

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-redis/redis/v7"
	"github.com/vmihailenco/msgpack/v4"
)

func TestPeekNext(t *testing.T) {
	r, m := dial(t)
	s := NewSync(r, m)

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"
	r.Del(":DIRTYQUE", ":DIRTYSET", key)

	if _, _, err := s.peek(); err != redis.Nil {
		t.Fatalf("unexpected peek error: %v", err)
	}

	b, _ := msgpack.Marshal(&data{Rev: 1, Val: val})
	r.Set(key, b2s(b), 0)
	r.SAdd(":DIRTYSET", key)
	r.LPush(":DIRTYQUE", key)

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
