package remon

import (
	"errors"
	"strings"
)

var (
	ErrAlreadyExists = errors.New("remon: already exists")
	ErrNotFound      = errors.New("remon: not found")
	ErrMailFull      = errors.New("remon: mail full")
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
