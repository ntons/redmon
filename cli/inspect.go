package main

import (
	"context"

	"github.com/go-redis/redis/v8"
	log "github.com/ntons/log-go"
)

func Inspect(ctx context.Context, args []string) {
	if len(args) == 0 {
		log.Fatalf("require inspect target")
	}
	switch args[0] {
	case "dirty":
		InspectDirty(ctx)
	}
}

func InspectDirty(ctx context.Context) {
	r := redis.NewClient(&redis.Options{Addr: cfg.Redis})
	pipe := r.Pipeline()
	c1 := pipe.LLen(ctx, ":DIRTYQUE")
	c2 := pipe.SCard(ctx, ":DIRTYSET")
	if _, err := pipe.Exec(ctx); err != nil {
		log.Fatalf("failed to exec: %v", err)
	}
	log.Infof("dirty queue size: %d, dirty set size: %d", c1.Val(), c2.Val())
}
