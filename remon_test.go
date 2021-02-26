package remon

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func rGetData(ctx context.Context, r *redis.Client, key string) (d xData) {
	b, _ := r.Get(ctx, key).Bytes()
	msgpack.Unmarshal(b, &d)
	return
}
func rSetData(ctx context.Context, r *redis.Client, key string, d xData) {
	b, _ := msgpack.Marshal(&d)
	r.Set(ctx, key, b2s(b), 0)
}

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

func TestReMonGet(t *testing.T) {
	r, m := dial(t)
	cli := New(r, m).(*xClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"

	r.Del(ctx, key)
	if _, _, err := cli.get(ctx, key); !isCacheMiss(err) {
		t.Fatalf("unexpected get err: %v", err)
	}

	rSetData(ctx, r, key, xData{Rev: 0, Val: ""})
	if _, _, err := cli.get(ctx, key); err != ErrNotFound {
		t.Fatalf("unexpected get err: %v", err)
	}

	rSetData(ctx, r, key, xData{Rev: 1, Val: val})
	if _, _val, err := cli.get(ctx, key); err != nil {
		t.Fatalf("unexpected get err: %v", err)
	} else if _val != val {
		t.Fatalf("unexpected get val: %v", _val)
	}

	rSetData(ctx, r, key, xData{Rev: 0, Val: ""})
	if _, _val, err := cli.get(ctx, key, AddIfNotFound(val)); err != nil {
		t.Fatalf("unexpected get err: %v", err)
	} else if _val != val {
		t.Fatalf("unexpected get val: %v", _val)
	}

	r.Del(ctx, key)
	if _, _val, err := cli.Get(ctx, key, AddIfNotFound(val)); err != nil {
		t.Fatalf("unexpected get err: %v", err)
	} else if _val != val {
		t.Fatalf("unexpected get val: %v", _val)
	}
}

func TestReMonSet(t *testing.T) {
	r, m := dial(t)
	cli := New(r, m).(*xClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"

	r.Del(ctx, key)
	if _, err := cli.set(ctx, key, val); !isCacheMiss(err) {
		t.Fatalf("unexpected set err: %v", err)
	}

	var d = xData{Rev: 0}
	b, _ := msgpack.Marshal(&d)
	r.Set(ctx, key, b2s(b), 0)
	if _, err := cli.set(ctx, key, val); err != nil {
		t.Fatalf("unexpected set err: %v", err)
	}
	b, _ = r.Get(ctx, key).Bytes()
	msgpack.Unmarshal(b, &d)
	if d.Rev != 1 {
		t.Fatalf("unexpected set rev: %v", d.Rev)
	}
	if d.Val != val {
		t.Fatalf("unexpected set val: %v", d.Val)
	}

	if _, err := cli.set(ctx, key, val); err != nil {
		t.Fatalf("unexpected set err: %v", err)
	}
	b, _ = r.Get(ctx, key).Bytes()
	msgpack.Unmarshal(b, &d)
	if d.Rev != 2 {
		t.Fatalf("unexpected set rev: %v", d.Rev)
	}
	if d.Val != val {
		t.Fatalf("unexpected set val: %v", d.Val)
	}
}

func TestReMonAdd(t *testing.T) {
	r, m := dial(t)
	cli := New(r, m).(*xClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"

	r.Del(ctx, key)
	if err := cli.add(ctx, key, val); !isCacheMiss(err) {
		t.Fatalf("unexpected add err: %v", err)
	}

	var d = xData{Rev: 0}
	b, _ := msgpack.Marshal(&d)
	r.Set(ctx, key, b2s(b), 0)
	if err := cli.add(ctx, key, val); err != nil {
		t.Fatalf("unexpected add err: %v", err)
	}
	if err := cli.add(ctx, key, val); err != ErrAlreadyExists {
		t.Fatalf("unexpected add err: %v", err)
	}
}

func TestReMonLoad(t *testing.T) {
	r, m := dial(t)
	cli := New(r, m).(*xClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var (
		database   = "test"
		collection = "remon"
		_id        = fmt.Sprintf("%d", rand.Int())
		key        = fmt.Sprintf("%s:%s:%s", database, collection, _id)
		val        = "hello"
	)

	r.Del(ctx, key)
	m.Database(database).Collection(collection).DeleteOne(
		ctx, bson.M{"_id": _id})
	if err := cli.load(ctx, key); err != nil {
		t.Fatalf("unexpected load err: %v", err)
	}
	if d := rGetData(ctx, r, key); d.Rev != 0 {
		t.Fatalf("unexpected load rev: %v", d.Rev)
	}

	r.Del(ctx, key)
	m.Database(database).Collection(collection).InsertOne(
		ctx, bson.M{"_id": _id, "rev": 1, "val": val})

	if err := cli.load(ctx, key); err != nil {
		t.Fatalf("unexpected load err: %v", err)
	}
	if d := rGetData(ctx, r, key); d.Rev != 1 {
		t.Fatalf("unexpected load rev: %v", d.Rev)
	} else if d.Val != val {
		t.Fatalf("unexpected load val: %v", d.Val)
	}
}

func TestReMonEval(t *testing.T) {
	r, m := dial(t)
	cli := New(r, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"
	defer r.Del(ctx, key)

	rSetData(ctx, r, key, xData{Rev: 1, Val: val})
	if s, err := cli.Eval(
		ctx, newEvalScript(`return VALUE`), key,
	).Text(); err != nil {
		t.Fatalf("unexpected eval error: %v", err)
	} else if s != val {
		t.Fatalf("unexpected eval value: %v", s)
	} else if d := rGetData(ctx, r, key); d.Rev != 1 {
		t.Fatalf("unexpected eval rev: %v", d.Rev)
	} else if d.Val != val {
		t.Fatalf("unexpected eval val: %v", d.Val)
	}

	if s, err := cli.Eval(
		ctx, newEvalScript(`VALUE="foo";return VALUE`), key,
	).Text(); err != nil {
		t.Fatalf("unexpected eval error: %v", err)
	} else if s != "foo" {
		t.Fatalf("unexpected eval value: %v", s)
	} else if d := rGetData(ctx, r, key); d.Rev != 2 {
		t.Fatalf("unexpected eval rev: %v", d.Rev)
	} else if d.Val != "foo" {
		t.Fatalf("unexpected eval val: %v", d.Val)
	}
}
