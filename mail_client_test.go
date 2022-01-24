package remon

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMailClient(t *testing.T) {
	r, m := dial(t)
	cli := NewMailClient(New(r, m))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"
	defer r.Del(ctx, key)

	r.Del(ctx, key)
	for i := int64(0); i < 10; i++ {
		if id, err := cli.Push(ctx, key, val); err != nil {
			t.Fatalf("unexpected push err: %v", err)
		} else if id != i+1 {
			t.Fatalf("unexpected push id: %v", id)
		}
	}

	if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 10 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := int64(0); i < 10; i++ {
			if list[i].Id != i+1 || list[i].Val != val {
				t.Fatalf("unexpected list elem: %v", list[1])
			}
		}
	}

	if pulled, err := cli.Pull(ctx, key, 1); err != nil {
		t.Fatalf("unexpected pull err: %v", err)
	} else if len(pulled) != 1 || pulled[0] != 1 {
		t.Fatalf("unexpected pull ret: %v", pulled)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 9 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := int64(0); i < 9; i++ {
			if list[i].Id != i+2 {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
	}

	if pulled, err := cli.Pull(ctx, key, 10); err != nil {
		t.Fatalf("unexpected pull err: %v", err)
	} else if len(pulled) != 1 || pulled[0] != 10 {
		t.Fatalf("unexpected pull ret: %v", pulled)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 8 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := int64(0); i < 8; i++ {
			if list[i].Id != i+2 {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
	}

	if pulled, err := cli.Pull(ctx, key, 5, 5, 7); err != nil {
		t.Fatalf("unexpected pull err: %v", err)
	} else if len(pulled) != 2 || pulled[0] != 5 || pulled[1] != 7 {
		t.Fatalf("unexpected pull ret: %v", pulled)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 6 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		a := []int64{2, 3, 4, 6, 8, 9}
		for i := int64(0); i < 6; i++ {
			if list[i].Id != a[i] {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
	}

	if err := cli.Clean(ctx, key); err != nil {
		t.Fatalf("unexpected drain err: %v", err)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 0 {
		t.Fatalf("unexpected list len: %v", len(list))
	}

	if id, err := cli.Push(ctx, key, val); err != nil {
		t.Fatalf("unexpected push err: %v", err)
	} else if id != 11 {
		t.Fatalf("unexpected push id: %v", id)
	}
}
