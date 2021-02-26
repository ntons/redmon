package remon

import (
	"errors"
	"strings"
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
	var s = strings.TrimSpace(err.Error())
	const s1 = "ERR Error running script"
	const s2 = "CACHE_MISS"
	return strings.HasPrefix(s, s1) && strings.HasSuffix(s, s2)
}
