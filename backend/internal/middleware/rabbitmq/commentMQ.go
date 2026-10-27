package rabbitmq

import (
	"context"
	"errors"
	"time"
)

// CommentMQ 负责「评论落库事件」的发布。
// Publish 允许调用方传入 event_id：服务层在发布前先生成 ID，无论最终走 MQ 还是
// 降级直写，都把同一个 ID 写进 comments 表，消费端才能靠唯一索引去重。
type CommentMQ struct {
	p *publisher
}

const (
	commentExchange   = "comment.events"
	commentQueue      = "comment.events"
	commentBindingKey = "comment.*"

	commentPublishRK = "comment.publish"
	commentDeleteRK  = "comment.delete"
)

type CommentEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	CommentID  uint      `json:"comment_id,omitempty"`
	Username   string    `json:"username,omitempty"`
	VideoID    uint      `json:"video_id,omitempty"`
	AuthorID   uint      `json:"author_id,omitempty"`
	Content    string    `json:"content,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewCommentMQ(base *RabbitMQ) (*CommentMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareTopic(ch, commentExchange, commentQueue, commentBindingKey); err != nil {
		ch.Close()
		return nil, err
	}
	p, err := newPublisher(ch, DefaultConfirmTimeout)
	if err != nil {
		ch.Close()
		return nil, err
	}
	return &CommentMQ{p: p}, nil
}

func (c *CommentMQ) Close() error {
	if c == nil || c.p == nil {
		return nil
	}
	return c.p.Close()
}

func (c *CommentMQ) Publish(ctx context.Context, username string, videoID, authorID uint, content, eventID string) error {
	return c.publish(ctx, "publish", commentPublishRK, CommentEvent{
		EventID:  eventID,
		Username: username,
		VideoID:  videoID,
		AuthorID: authorID,
		Content:  content,
	})
}

func (c *CommentMQ) Delete(ctx context.Context, commentID uint) error {
	return c.publish(ctx, "delete", commentDeleteRK, CommentEvent{
		CommentID: commentID,
	})
}

func (c *CommentMQ) publish(ctx context.Context, action, routingKey string, evt CommentEvent) error {
	if c == nil || c.p == nil {
		return errors.New("comment mq is not initialized")
	}
	if evt.EventID == "" {
		id, err := newEventID(16)
		if err != nil {
			return err
		}
		evt.EventID = id
	}
	evt.Action = action
	evt.OccurredAt = time.Now().UTC()
	return c.p.PublishJSON(ctx, commentExchange, routingKey, evt)
}
