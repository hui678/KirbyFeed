package redis

import (
	"context"
	"errors"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func (c *Client) ZincrBy(ctx context.Context, key string, member string, score float64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZIncrBy(ctx, key, score, member).Err()
}

func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

// ZRem 删除若干 member，不存在时返回 nil（幂等）。
func (c *Client) ZRem(ctx context.Context, key string, members ...string) error {
	if c == nil || c.rdb == nil || len(members) == 0 {
		return nil
	}
	args := make([]interface{}, len(members))
	for i, member := range members {
		args[i] = member
	}
	return c.rdb.ZRem(ctx, key, args...).Err()
}

func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
}

func (c *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.ZRangeWithScores(ctx, key, start, stop).Result()
}

func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Expire(ctx, key, ttl).Err()
}

func (c *Client) ZUnionStore(ctx context.Context, dst string, keys []string, aggregate string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
		Keys:      keys,
		Aggregate: aggregate,
	}).Err()
}

func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRange(ctx, key, start, stop).Result()
}

func (c *Client) ZRevRangeByScore(ctx context.Context, key string, max, min string, offset, count int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: offset,
		Count:  count,
	}).Result()
}

// ZCount 统计 [min, max] 区间内的成员数，用于计算某个 score 上有多少条重复数据
func (c *Client) ZCount(ctx context.Context, key string, min, max string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, errors.New("redis client not initialized")
	}
	return c.rdb.ZCount(ctx, key, min, max).Result()
}

// ZCard 返回集合基数。key 不存在时返回 0 且不报错，用来区分「空窗口」和「读取失败」。
func (c *Client) ZCard(ctx context.Context, key string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, errors.New("redis client not initialized")
	}
	return c.rdb.ZCard(ctx, key).Result()
}

// ZRevRangeByScoreWithScores 与 ZRevRangeByScore 相同，但一并返回 score。
// 时间线游标需要 (score, member) 两个值才能做复合游标，光有 member 不够。
func (c *Client) ZRevRangeByScoreWithScores(ctx context.Context, key string, max, min string, offset, count int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.ZRevRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: offset,
		Count:  count,
	}).Result()
}
