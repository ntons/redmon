package remon

import (
	"errors"
	"strings"

	"github.com/go-redis/redis/v8"
)

var (
	ErrAlreadyExists = errors.New("remon: already exists")
	ErrNotFound      = errors.New("remon: not found")
	ErrMailFull      = errors.New("remon: mail full")
)

func isCacheMiss(err error) bool {
	return err == redis.Nil
}

func isNoScript(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ")
}
