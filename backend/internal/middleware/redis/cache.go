package redis

import (
	"context"
	"errors"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.Get(ctx, key).Bytes()
}

func (c *Client) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

func (c *Client) Del(ctx context.Context, key string) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.Del(ctx, key).Err()
}

func (c *Client) DelByPattern(ctx context.Context, pattern string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	iter := c.rdb.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		_ = c.rdb.Del(ctx, iter.Val())
	}
	return iter.Err()
}

func (c *Client) MGet(cacheCtx context.Context, cacheKeys ...string) ([]interface{}, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.MGet(cacheCtx, cacheKeys...).Result()
}

// Eval 执行一段 Lua 脚本并返回其整数结果。
//
// 存在的意义是把「去重判断」和「副作用」合成一次原子操作：Redis 执行 Lua 是单线程
// 原子的，所以不存在「已标记为处理过、但副作用没执行」的中间态。
// 用两条独立命令（SETNX 再 ZINCRBY）就必然有这个窗口，而它一旦发生就是永久丢数据
// ——后续重试会被去重标记挡住，副作用再也不会执行。
func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, errors.New("redis client not initialized")
	}
	v, err := c.rdb.Eval(ctx, script, keys, args...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok {
		return 0, nil
	}
	return n, nil
}

// ── 位图（Bitmap）原语 ──
// 分片上传会话用真实 Redis 位图记录「哪些分片已上传」：
// SETBIT/GETBIT 是单 bit 原子操作，避免「读整个会话 JSON → 改一位 → 写回」的
// read-modify-write 竞态（并发上传分片时后写覆盖前写）。

func (c *Client) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("redis client not initialized")
	}
	return c.rdb.SetNX(ctx, key, value, ttl).Result()
}

func (c *Client) PExpire(ctx context.Context, key string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.PExpire(ctx, key, ttl).Err()
}

// setBitWithTTLScript 把置位和续期合成一次原子操作。
// pipeline 只是批量发送，不是原子执行：SETBIT 成功而 PEXPIRE 失败仍会留下永不过期的 key。
var setBitWithTTLScript = redis.NewScript(`
redis.call("SETBIT", KEYS[1], ARGV[1], ARGV[2])
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return 1
`)

// SetBitWithTTL 置位并刷新 TTL，一次往返完成，避免「置位成功但 PEXPIRE 失败」留下永不过期的 key。
func (c *Client) SetBitWithTTL(ctx context.Context, key string, offset int64, value int, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return setBitWithTTLScript.Run(
		ctx, c.rdb, []string{key}, offset, value, ttl.Milliseconds(),
	).Err()
}

// PExpireMany 用一个 pipeline 给一组 key 续期。
// 分片上传的会话、位图、hash 索引必须一起续：只续其中一部分会让三者寿命错位。
func (c *Client) PExpireMany(ctx context.Context, ttl time.Duration, keys ...string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	if len(keys) == 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	for _, key := range keys {
		pipe.PExpire(ctx, key, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("redis client not initialized")
	}
	n, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// compareAndDeleteScript 只在值仍等于期望值时才删除。
// 用于「映射仍指向我这个会话」的清理：并发 re-init 可能已经把它改指向新会话，
// 无条件 DEL 会把别人的映射删掉。
var compareAndDeleteScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

func (c *Client) CompareAndDelete(ctx context.Context, key string, expected []byte) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	n, err := compareAndDeleteScript.Run(ctx, c.rdb, []string{key}, string(expected)).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (c *Client) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, errors.New("redis client not initialized")
	}
	return c.rdb.GetBit(ctx, key, offset).Result()
}

// GetBits 一次性读出位图前 n 位（pipeline 合并成一次往返）。
func (c *Client) GetBits(ctx context.Context, key string, n int) ([]bool, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	if n <= 0 {
		return nil, nil
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, n)
	for i := 0; i < n; i++ {
		cmds = append(cmds, pipe.GetBit(ctx, key, int64(i)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	bits := make([]bool, 0, n)
	for _, cmd := range cmds {
		bits = append(bits, cmd.Val() == 1)
	}
	return bits, nil
}

func (c *Client) BitCount(ctx context.Context, key string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, errors.New("redis client not initialized")
	}
	return c.rdb.BitCount(ctx, key, nil).Result()
}
