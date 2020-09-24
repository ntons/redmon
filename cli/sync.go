package main

import (
	"context"
	"sync"

	log "github.com/ntons/log-go"
	remon "github.com/ntons/remon-go"
)

func Sync(ctx context.Context) {
	r, m, err := Dial()
	if err != nil {
		log.Fatalf("failed to dail: %v", err)
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	s := remon.NewSync(r, m, remon.WithSyncRate(cfg.Sync.Rate))
	defer s.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Serve()
	}()

	<-ctx.Done()

}
