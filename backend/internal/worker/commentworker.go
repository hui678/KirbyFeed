package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"

	amqp "github.com/rabbitmq/amqp091-go"
)

// CommentWorker 消费「评论落库事件」。
// 幂等靠 comments.event_id 唯一索引：消息重复投递时 INSERT IGNORE 的 RowsAffected
// 为 0，直接返回，不会产生第二条评论。
type CommentWorker struct {
	comments *video.CommentRepository
	queue    string
	retry    RetryPublisher
}

func NewCommentWorker(comments *video.CommentRepository, queue string, retry RetryPublisher) *CommentWorker {
	return &CommentWorker{comments: comments, queue: queue, retry: retry}
}

func (w *CommentWorker) Run(ctx context.Context, ch *amqp.Channel) error {
	if w == nil || w.comments == nil {
		return errors.New("comment worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}
	c := &Consumer{
		Name:    "CommentWorker",
		Queue:   w.queue,
		Retry:   w.retry,
		Process: w.process,
	}
	return c.Run(ctx, ch)
}

func (w *CommentWorker) process(ctx context.Context, d amqp.Delivery) error {
	var evt rabbitmq.CommentEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		// 坏消息，重试无意义
		return nil
	}
	switch evt.Action {
	case "publish":
		return w.applyPublish(ctx, &evt)
	case "delete":
		return w.applyDelete(ctx, &evt)
	default:
		return nil
	}
}

func (w *CommentWorker) applyPublish(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	if evt == nil || evt.VideoID == 0 || evt.AuthorID == 0 || strings.TrimSpace(evt.Content) == "" {
		return nil
	}

	c := &video.Comment{
		Username: strings.TrimSpace(evt.Username),
		VideoID:  evt.VideoID,
		AuthorID: evt.AuthorID,
		Content:  strings.TrimSpace(evt.Content),
	}
	// 幂等键：没有 event_id 的旧消息退化为「不去重」，但不会报错。
	if eid := strings.TrimSpace(evt.EventID); eid != "" {
		c.EventID = &eid
	}
	return w.comments.ApplyPublish(ctx, c)
}

func (w *CommentWorker) applyDelete(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	if evt == nil || evt.CommentID == 0 {
		return nil
	}
	// 删除天然幂等：行已经不在就什么都不做。
	return w.comments.ApplyDelete(ctx, evt.CommentID)
}
