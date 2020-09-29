package mailbox

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/remon-go"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

func dial(t *testing.T) (*redis.Client, *mongo.Client) {
	r := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	m, err := mongo.NewClient(
		mongooptions.Client().ApplyURI("mongodb://127.0.0.1"))
	if err != nil {
		t.Fatal("failed to new mongo client:", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Connect(ctx); err != nil {
		t.Fatal("failed to connect mongo server:", err)
	}
	return r, m
}
func TestMailbox(t *testing.T) {
	r, m := dial(t)
	cli := New(remon.New(r, m))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"
	defer r.Del(ctx, key)

	r.Del(ctx, key)
	for i := 0; i < 10; i++ {
		if id, err := cli.Push(ctx, key, val); err != nil {
			t.Fatalf("unexpected push err: %v", err)
		} else if id != fmt.Sprintf("%08X", i+1) {
			t.Fatalf("unexpected push id: %v", id)
		}
	}

	if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 10 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := 0; i < 10; i++ {
			if list[i].Id != fmt.Sprintf("%08X", i+1) || list[i].Content != val {
				t.Fatalf("unexpected list elem: %v", list[1])
			}
		}
	}

	if err := cli.Pull(ctx, key, "00000001"); err != nil {
		t.Fatalf("unexpected pull err: %v", err)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 9 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := 0; i < 9; i++ {
			if list[i].Id != fmt.Sprintf("%08X", i+2) {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
	}

	if err := cli.Pull(ctx, key, "0000000A"); err != nil {
		t.Fatalf("unexpected pull err: %v", err)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 8 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := 0; i < 8; i++ {
			if list[i].Id != fmt.Sprintf("%08X", i+2) {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
	}

	if err := cli.Pull(ctx, key, "00000007"); err != nil {
		t.Fatalf("unexpected pull err: %v", err)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 7 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := 0; i < 5; i++ {
			if list[i].Id != fmt.Sprintf("%08X", i+2) {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
		for i := 5; i < 7; i++ {
			if list[i].Id != fmt.Sprintf("%08X", i+3) {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
	}

	if err := cli.Drain(ctx, key); err != nil {
		t.Fatalf("unexpected drain err: %v", err)
	} else if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 0 {
		t.Fatalf("unexpected list len: %v", len(list))
	}

	if id, err := cli.Push(ctx, key, val); err != nil {
		t.Fatalf("unexpected push err: %v", err)
	} else if id != "0000000B" {
		t.Fatalf("unexpected push id: %v", id)
	}

}
