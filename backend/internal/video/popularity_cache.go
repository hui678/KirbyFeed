package video

import (
	"context"
	"strconv"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

// 热度缓存相关的 TTL。
const (
	// hotWindowTTL 分钟桶的存活时间。要大于热榜快照的合并窗口（60 分钟），
	// 否则合并时最老的桶已经没了，热度会算少。
	hotWindowTTL = 2 * time.Hour
	// hotDedupTTL 热度事件幂等标记的存活时间。要大于「一条消息最长可能重投多久」
	// （进程内退避 3 次 + 重试队列 3 轮 TTL），否则标记先过期、重投又被当成新事件。
	hotDedupTTL = 24 * time.Hour
)

// hotIncrOnceScript 是「幂等累加」的核心：去重标记、累加、续期三步一次原子完成。
//
// KEYS[1] 幂等标记 key
// KEYS[2] 分钟桶 key
// ARGV[1] member（video_id）
// ARGV[2] change（增量）
// ARGV[3] 幂等标记 TTL（秒）
// ARGV[4] 分钟桶 TTL（秒）
//
// 返回 1 = 本次真的累加了；返回 0 = 这个事件之前处理过，跳过。
//
// 为什么必须用 Lua 而不是「先 SETNX 再 ZINCRBY」两条命令：
// 两条命令之间进程崩溃，就会留下「已标记为处理过、但累加没执行」的状态，
// 之后所有重试都被去重挡住，这次热度**永久丢失**。
// Redis 执行 Lua 是单线程原子的，要么三步全做、要么全不做，不存在这个中间态。
const hotIncrOnceScript = `
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[3]) == false then
  return 0
end
redis.call('ZINCRBY', KEYS[2], ARGV[2], ARGV[1])
redis.call('EXPIRE', KEYS[2], ARGV[4])
return 1
`

// UpdatePopularityCache 更新视频热度缓存：失效实体缓存 + 写分钟窗 ZSET。
//
// 返回 error 是刻意的：调用方（热度 Worker、服务层降级路径）必须能感知 Redis 失败。
// 旧实现吞掉错误后照样 Ack，Redis 故障期间热度事件被静默丢弃，热榜就不是
// 「最终一致」而是「永久丢失」。
//
// eventID 是幂等键，用于消化「先处理再 ACK，崩溃后重投」造成的重复：
//   - 消费端传消息里的 evt.EventID；
//   - 服务层降级路径传**发布前生成的同一个** eventID。
//
// 两者共用同一个 ID 是关键：MQ 发布失败往往只是 confirm 超时，消息其实已经在队列里。
// 降级直写和消费端各写一次就会把热度加两遍 —— 而 MySQL 侧的 popularity 有
// likes/comments 的唯一索引保护只加一遍，两边就对不上了。
//
// eventID 为空时退化为不做去重（保持旧行为），保证历史调用点不会因为漏传而失效。
func UpdatePopularityCache(ctx context.Context, cache *rediscache.Client, id uint, change int64, eventID string) error {
	if cache == nil || id == 0 || change == 0 {
		return nil
	}

	InvalidateVideoEntity(ctx, cache, id)

	now := time.Now().UTC().Truncate(time.Minute)
	windowKey := cache.Key("hot:video:1m:%s", now.Format("200601021504"))
	member := strconv.FormatUint(uint64(id), 10)

	// 每个 Redis 操作各自申请独立超时预算：带 timeout 的 context 的额度会被
	// 串联在上面的操作连续消耗，共用的话后一步会因为预算耗尽而静默失败。
	if eventID != "" {
		incrCtx, incrCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		defer incrCancel()
		_, err := cache.Eval(incrCtx, hotIncrOnceScript,
			[]string{cache.Key("hot:dedup:%s", eventID), windowKey},
			member, change,
			int64(hotDedupTTL/time.Second), int64(hotWindowTTL/time.Second))
		return err
	}

	incrCtx, incrCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
	err := cache.ZincrBy(incrCtx, windowKey, member, float64(change))
	incrCancel()
	if err != nil {
		return err
	}

	expireCtx, expireCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
	err = cache.Expire(expireCtx, windowKey, hotWindowTTL)
	expireCancel()
	return err
}
