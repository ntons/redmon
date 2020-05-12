package remon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

func Dial(t *testing.T) *ReMon {
	r := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if _, err := r.Ping().Result(); err != nil {
		t.Fatal("failed to ping redis:", err)
	}

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

	if err := ScriptLoad(r); err != nil {
		t.Fatal("failed to script load:", err)
	}

	return New(r, m)
}

func TestLock(t *testing.T) {
	x := Dial(t)
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fmt.Println(x.Get(ctx, "aa:bb:cc", AddOnNotExist("hello")))
		fmt.Println(x.Stat())
	}()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fmt.Println(x.Set(ctx, "aa:bb:cc", "world"))
	}()
}
