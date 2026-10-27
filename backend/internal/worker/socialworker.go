package worker

import (
	"context"
	"encoding/json"
	"errors"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/social"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SocialWorker 消费「关注/取关事件」。
// 幂等靠 (follower_id, vlogger_id) 唯一索引 + RowsAffected 判断，
// 不再靠「吞掉 1062 错误」这种隐式方式。
type SocialWorker struct {
	repo  *social.SocialRepository
	queue string
	retry RetryPublisher
}

func NewSocialWorker(repo *social.SocialRepository, queue string, retry RetryPublisher) *SocialWorker {
	return &SocialWorker{repo: repo, queue: queue, retry: retry}
}

func (w *SocialWorker) Run(ctx context.Context, ch *amqp.Channel) error {
	if w == nil || w.repo == nil {
		return errors.New("social worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}
	c := &Consumer{
		Name:    "SocialWorker",
		Queue:   w.queue,
		Retry:   w.retry,
		Process: w.process,
	}
	return c.Run(ctx, ch)
}

func (w *SocialWorker) process(ctx context.Context, d amqp.Delivery) error {
	var evt rabbitmq.SocialEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		// 坏消息，重试无意义
		return nil
	}
	if evt.FollowerID == 0 || evt.VloggerID == 0 {
		return nil
	}

	switch evt.Action {
	case "follow":
		_, err := w.repo.ApplyFollow(ctx, &social.Social{
			FollowerID: evt.FollowerID,
			VloggerID:  evt.VloggerID,
		})
		return err
	case "unfollow":
		_, err := w.repo.ApplyUnfollow(ctx, &social.Social{
			FollowerID: evt.FollowerID,
			VloggerID:  evt.VloggerID,
		})
		return err
	default:
		return nil
	}
}
