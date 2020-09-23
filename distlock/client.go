package redislock

import (
	"time"
)

type Client struct {
	r RedisClient
}

func NewClient(r RedisClient) *Client {
	return &Client{r: r}
}

func (cli Client) Lock(key string, expire time.Time, opts ...LockOption) (string, error) {
	return Lock(cli.r, key, expire, opts...)
}

func (cli Client) Unlock(key, token string, opts ...UnlockOption) error {
	return Unlock(cli.r, key, token, opts...)
}

func (cli Client) Verify(key, token string) error {
	return Verify(cli.r, key, token)
}

func (cli Client) Refresh(key, token string, expire time.Time) error {
	return Refresh(cli.r, key, token, expire)
}
