package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"feedsystem_video_go/internal/config"
	"fmt"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Client struct {
	rdb       *redis.Client
	keyPrefix string
}

const defaultKeyPrefix = "v1:"

func NewClient(rdb *redis.Client, keyPrefix string) *Client {
	return &Client{rdb: rdb, keyPrefix: keyPrefix}
}

func NewFromEnv(cfg *config.RedisConfig) (*Client, error) {
	cfg.Normalize()
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,

		// 默认 ContextTimeoutEnabled=false：上层传入的 ctx 超时不会作用到 socket
		// I/O 上，Redis 抖动时请求会一直挂到 ReadTimeout 才返回。缓存本该在
		// 故障时快速失败、让请求回源，而不是把 Redis 的延迟放大到整个接口。
		ContextTimeoutEnabled: true,

		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		// 池被占满时最多等 200ms 就放弃，回源兜底，而不是无限排队。
		PoolTimeout: 200 * time.Millisecond,
		DialTimeout: time.Second,
		// 没有 ctx 截止时间的调用以这两个值为兜底上限。
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		// 只重试一次：Redis 通常是本地/同可用区，重试多次只会在故障时放大流量。
		MaxRetries:      1,
		ConnMaxIdleTime: 5 * time.Minute,
	})
	return &Client{rdb: rdb, keyPrefix: defaultKeyPrefix}, nil
}

func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.Ping(ctx).Err()
}

func IsMiss(err error) bool {
	return err == redis.Nil
}

func (c *Client) Key(format string, args ...any) string {
	prefix := ""
	if c != nil {
		prefix = c.keyPrefix
	}
	return prefix + fmt.Sprintf(format, args...)
}

func randToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) Lock(ctx context.Context, key string, ttl time.Duration) (token string, ok bool, err error) {
	if c == nil || c.rdb == nil {
		return "", false, nil
	}
	token, err = randToken(16)
	if err != nil {
		return "", false, err
	}
	ok, err = c.rdb.SetNX(ctx, key, token, ttl).Result()
	return token, ok, err
}

var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

var incrementWithExpireScript = redis.NewScript(`
local count = redis.call("INCR", KEYS[1])
if count == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return count
`)

func (c *Client) Unlock(ctx context.Context, key string, token string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	_, err := unlockScript.Run(ctx, c.rdb, []string{key}, token).Result()
	return err
}

// refreshLockScript 只在自己仍持有锁时续期。
// 直接 PEXPIRE 会在锁已被别人抢到后把对方的租约一起改掉，等于伪造了一把锁。
var refreshLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`)

func (c *Client) RefreshLock(ctx context.Context, key string, token string, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	n, err := refreshLockScript.Run(ctx, c.rdb, []string{key}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (c *Client) IncrementWithExpire(ctx context.Context, key string, expire time.Duration) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, nil
	}
	return incrementWithExpireScript.Run(
		ctx,
		c.rdb,
		[]string{key},
		expire.Milliseconds(),
	).Int64()
}
