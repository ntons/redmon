package remon

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ntons/redis"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func rGetData(ctx context.Context, r redis.Client, key string) (d xRedisData) {
	b, _ := r.Get(ctx, key).Bytes()
	msgpack.Unmarshal(b, &d)
	return
}
func rSetData(ctx context.Context, r redis.Client, key string, d xRedisData) {
	b, _ := msgpack.Marshal(&d)
	r.Set(ctx, key, b2s(b), 0)
}

func dial(t *testing.T) (redis.Client, *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := redis.Dial(ctx, "redis://127.0.0.1:6379")
	if err != nil {
		t.Fatal("failed to new redis client:", err)
	}
	m, err := mongo.NewClient(
		mongooptions.Client().ApplyURI("mongodb://127.0.0.1"))
	if err != nil {
		t.Fatal("failed to new mongo client:", err)
	}
	if err := m.Connect(ctx); err != nil {
		t.Fatal("failed to connect mongo server:", err)
	}
	return r, m
}

func TestGet(t *testing.T) {
	r, m := dial(t)
	cli := NewClient(r, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = "hello", "world"
	defer r.Del(ctx, key)

	r.Del(ctx, key)
	if _, _, err := cli.rget(ctx, key, xGetOptions{}); err != redis.Nil {
		t.Fatalf("unexpected get err: %v", err)
	}

	rSetData(ctx, r, key, xRedisData{Rev: 0, Val: ""})
	if rev, _, err := cli.rget(ctx, key, xGetOptions{}); err != ErrNotExists {
		t.Fatalf("unexpected get err: %v, %v", rev, err)
	}

	rSetData(ctx, r, key, xRedisData{Rev: 1, Val: val})
	if _, _val, err := cli.rget(ctx, key, xGetOptions{}); err != nil {
		t.Fatalf("unexpected get err: %v", err)
	} else if _val != val {
		t.Fatalf("unexpected get val: %v", _val)
	}

	rSetData(ctx, r, key, xRedisData{Rev: 0, Val: ""})
	if _, _val, err := cli.rget(ctx, key, xGetOptions{addIfNotExists: &val}); err != nil {
		t.Fatalf("unexpected get err: %v", err)
	} else if _val != val {
		t.Fatalf("unexpected get val: %v", _val)
	}

	r.Del(ctx, key)
	if _, _val, err := cli.Get(ctx, key, AddIfNotExists(val)); err != nil {
		t.Fatalf("unexpected get err: %v", err)
	} else if _val != val {
		t.Fatalf("unexpected get val: %v", _val)
	}
}

func TestSet(t *testing.T) {
	r, m := dial(t)
	cli := NewClient(r, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = "hello", "world"
	defer r.Del(ctx, key)

	r.Del(ctx, key)
	if _, err := cli.rset(ctx, key, val); err != redis.Nil {
		t.Fatalf("unexpected set err: %v", err)
	}

	var d = xRedisData{Rev: 0}
	b, _ := msgpack.Marshal(&d)
	r.Set(ctx, key, b2s(b), 0)
	if _, err := cli.rset(ctx, key, val); err != nil {
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

	if _, err := cli.rset(ctx, key, val); err != nil {
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

func TestAdd(t *testing.T) {
	r, m := dial(t)
	cli := NewClient(r, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = "hello", "world"
	defer r.Del(ctx, key)

	r.Del(ctx, key)
	if err := cli.radd(ctx, key, val); err != redis.Nil {
		t.Fatalf("unexpected add err: %v", err)
	}

	var d = xRedisData{Rev: 0}
	b, _ := msgpack.Marshal(&d)
	r.Set(ctx, key, b2s(b), 0)
	if err := cli.radd(ctx, key, val); err != nil {
		t.Fatalf("unexpected add err: %v", err)
	}
	if err := cli.radd(ctx, key, val); err != ErrAlreadyExists {
		t.Fatalf("unexpected add err: %v", err)
	}
}

func TestLoad(t *testing.T) {
	r, m := dial(t)
	cli := NewClient(r, m)

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
		ctx, bson.M{"_id": _id, "rev": 1, "val": []byte(val)})

	if err := cli.load(ctx, key); err != nil {
		t.Fatalf("unexpected load err: %v", err)
	}
	if d := rGetData(ctx, r, key); d.Rev != 1 {
		t.Fatalf("unexpected load rev: %v", d.Rev)
	} else if d.Val != val {
		t.Fatalf("unexpected load val: %v", d.Val)
	}
}

func TestMail(t *testing.T) {
	r, m := dial(t)
	cli := NewClient(r, m)

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

	if id, err := cli.Push(ctx, key, val); err != nil {
		t.Fatalf("unexpected push err: %v", err)
	} else if id != 11 {
		t.Fatalf("unexpected push id: %v", id)
	}
}

func TestMail2(t *testing.T) {
	r, m := dial(t)
	cli := NewClient(r, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"
	defer r.Del(ctx, key)

	r.Del(ctx, key)

	for i := int64(0); i < 10; i++ {
		if id, err := cli.Push(ctx, key, val, WithImportance(int(i))); err != nil {
			t.Fatalf("unexpected push err: %v", err)
		} else if id != i*1e10+i+1 {
			t.Fatalf("unexpected push id: %v", id)
		}
	}

	if list, err := cli.List(ctx, key); err != nil {
		t.Fatalf("unexpected list err: %v", err)
	} else if len(list) != 10 {
		t.Fatalf("unexpected list len: %v", len(list))
	} else {
		for i := int64(0); i < 10; i++ {
			if list[i].Id != i*1e10+i+1 || list[i].Val != val {
				t.Fatalf("unexpected list elem: %v", list[i])
			}
		}
	}
}

func TestSync(t *testing.T) {
	const (
		xDirtySet = "$DIRTYSET$"
		xDirtyQue = "$DIRTYQUE$"
	)
	r, m := dial(t)
	cli := NewClient(r, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var key, val = fmt.Sprintf("%d", rand.Int()), "hello"
	r.Del(ctx, xDirtyQue, xDirtySet, key)

	if _, _, err := cli.peek(ctx); err != redis.Nil {
		t.Fatalf("unexpected peek error: %v", err)
	}

	b, _ := msgpack.Marshal(&xRedisData{Rev: 1, Val: val})
	r.Set(ctx, key, b2s(b), 0)
	r.SAdd(ctx, xDirtySet, key)
	r.LPush(ctx, xDirtyQue, key)

	if k, d, err := cli.peek(ctx); err != nil {
		t.Fatalf("unexpected peek error: %v", err)
	} else if k != key {
		t.Fatalf("unexpected peek key: %v", k)
	} else if d.Rev != 1 {
		t.Fatalf("unexpected peek rev: %v", d.Rev)
	} else if d.Val != val {
		t.Fatalf("unexpected peek val: %v", d.Val)
	}

	if k, d, err := cli.next(ctx, key, 2); err != nil {
		t.Fatalf("unexpected next error: %v", err)
	} else if k != key {
		t.Fatalf("unexpected next key: %v", k)
	} else if d.Rev != 1 {
		t.Fatalf("unexpected next rev: %v", d.Rev)
	} else if d.Val != val {
		t.Fatalf("unexpected next val: %v", d.Val)
	}

	if _, _, err := cli.next(ctx, key, 1); err != redis.Nil {
		t.Fatalf("unexpected next error: %v", err)
	}
}
