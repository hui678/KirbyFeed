package worker

import (
	"context"
	"encoding/json"
	"errors"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"

	amqp "github.com/rabbitmq/amqp091-go"
)

// LikeWorker 消费「点赞落库事件」，只负责 MySQL 里的 likes 行与 videos 计数。
// 热度（Redis 分钟桶）由 PopularityWorker 消费另一条消息完成，两者互不影响。
type LikeWorker struct {
	likes *video.LikeRepository
	queue string
	retry RetryPublisher
}

func NewLikeWorker(likes *video.LikeRepository, queue string, retry RetryPublisher) *LikeWorker {
	return &LikeWorker{likes: likes, queue: queue, retry: retry}
}

func (w *LikeWorker) Run(ctx context.Context, ch *amqp.Channel) error {
	if w == nil || w.likes == nil {
		return errors.New("like worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}
	c := &Consumer{
		Name:    "LikeWorker",
		Queue:   w.queue,
		Retry:   w.retry,
		Process: w.process,
	}
	return c.Run(ctx, ch)
}

func (w *LikeWorker) process(ctx context.Context, d amqp.Delivery) error {
	var evt rabbitmq.LikeEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		// 结构性坏消息（不是业务失败）：重试多少次都解不开，直接 Ack 丢弃，
		// 否则它会占满重试次数再进死信，制造无意义的告警。
		return nil
	}
	if evt.UserID == 0 || evt.VideoID == 0 {
		return nil
	}

	switch evt.Action {
	case "like":
		return w.likes.ApplyLike(ctx, evt.UserID, evt.VideoID, evt.EventID)
	case "unlike":
		return w.likes.ApplyUnlike(ctx, evt.UserID, evt.VideoID, evt.EventID)
	default:
		return nil
	}
}
