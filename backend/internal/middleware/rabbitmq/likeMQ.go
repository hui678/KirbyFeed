package rabbitmq

import (
	"context"
	"errors"
	"time"
)

// LikeMQ 负责「点赞落库事件」的发布。
// Channel 开启了 publisher confirm：publish 返回 nil 才代表 broker 已确认收下，
// 服务层据此决定「走异步」还是「降级直写」。
type LikeMQ struct {
	p *publisher
}

const (
	likeExchange   = "like.events"
	likeQueue      = "like.events"
	likeBindingKey = "like.*"

	likeLikeRK   = "like.like"
	likeUnlikeRK = "like.unlike"
)

type LikeEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	UserID     uint      `json:"user_id"`
	VideoID    uint      `json:"video_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewLikeMQ(base *RabbitMQ) (*LikeMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareTopic(ch, likeExchange, likeQueue, likeBindingKey); err != nil {
		ch.Close()
		return nil, err
	}
	p, err := newPublisher(ch, DefaultConfirmTimeout)
	if err != nil {
		ch.Close()
		return nil, err
	}
	return &LikeMQ{p: p}, nil
}

func (l *LikeMQ) Close() error {
	if l == nil || l.p == nil {
		return nil
	}
	return l.p.Close()
}

func (l *LikeMQ) Like(ctx context.Context, userID, videoID uint) error {
	return l.publish(ctx, "like", likeLikeRK, userID, videoID)
}

func (l *LikeMQ) Unlike(ctx context.Context, userID, videoID uint) error {
	return l.publish(ctx, "unlike", likeUnlikeRK, userID, videoID)
}

func (l *LikeMQ) publish(ctx context.Context, action, routingKey string, userID, videoID uint) error {
	if l == nil || l.p == nil {
		return errors.New("like mq is not initialized")
	}
	if userID == 0 || videoID == 0 {
		return errors.New("userID and videoID are required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	event := LikeEvent{
		EventID:    id,
		Action:     action,
		UserID:     userID,
		VideoID:    videoID,
		OccurredAt: time.Now(),
	}
	return l.p.PublishJSON(ctx, likeExchange, routingKey, event)
}
