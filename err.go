package remon

import (
	"errors"
	"strings"
)

const (
	eCacheMiss = "CACHE_MISS"
)

var (
	ErrAlreadyExists = errors.New("already exists")
	ErrNotFound      = errors.New("not found")
	errCacheMiss     = errors.New("cache miss")
)

func isCacheMiss(err error) bool {
	if err == nil {
		return false
	}
	if err == errCacheMiss {
		return true
	}
	s := strings.TrimSpace(err.Error())
	return strings.HasPrefix(s, "ERR Error running script") &&
		strings.HasSuffix(s, "CACHE_MISS")
}
func isBusy(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "BUSY")
}
