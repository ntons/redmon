package remon

import (
	"errors"
	"strings"
)

var (
	ErrAlreadyExists = errors.New("already exists")
	ErrNotFound      = errors.New("not found")
	ErrMailFull      = errors.New("mail full")
	// internal errors
	xErrCacheMiss = errors.New("cache miss")
)

func isCacheMiss(err error) bool {
	if err == nil {
		return false
	}
	if err == xErrCacheMiss {
		return true
	}
	var str = strings.TrimSpace(err.Error())
	return strings.HasPrefix(str, "ERR Error running script") &&
		strings.HasSuffix(str, "CACHE_MISS")
}
