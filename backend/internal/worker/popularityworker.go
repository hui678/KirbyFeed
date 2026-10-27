package worker

import (
	"context"
	"encoding/json"
	"errors"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PopularityWorker 消费「热度事件」，只写 Redis（失效实体缓存 + 分钟桶 ZSET）。
// 它与落库 Worker 完全独立：Redis 挂了不会拖慢 MySQL 落库，MySQL 慢了也不会
// 让热度事件堆积在落库队列里。
type PopularityWorker struct {
	cache *rediscache.Client
	queue string
	retry RetryPublisher
}

func NewPopularityWorker(cache *rediscache.Client, queue string, retry RetryPublisher) *PopularityWorker {
	return &PopularityWorker{cache: cache, queue: queue, retry: retry}
}

func (w *PopularityWorker) Run(ctx context.Context, ch *amqp.Channel) error {
	if w == nil || w.cache == nil {
		return errors.New("popularity worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}
	c := &Consumer{
		Name:    "PopularityWorker",
		Queue:   w.queue,
		Retry:   w.retry,
		Process: w.process,
	}
	return c.Run(ctx, ch)
}

func (w *PopularityWorker) process(ctx context.Context, d amqp.Delivery) error {
	var evt rabbitmq.PopularityEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		// 坏消息，重试无意义
		return nil
	}
	if evt.VideoID == 0 || evt.Change == 0 {
		return nil
	}
	// 必须把 Redis 错误抛出去：旧实现吞掉错误后照样 Ack，
	// Redis 故障期间所有热度事件被静默丢弃，热榜根本谈不上「最终一致」。
	//
	// 传 evt.EventID 做幂等：ZINCRBY 是累加、天生不幂等，「先处理再 ACK、崩溃重投」
	// 会把热榜刷高一分。去重的原子性由 UpdatePopularityCache 里的 Lua 脚本保证。
	return video.UpdatePopularityCache(ctx, w.cache, evt.VideoID, evt.Change, evt.EventID)
}
