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

// Like / Unlike 的 eventID 由调用方传入：服务层在发布前生成，
// MQ 路径与降级直写共用同一个，消费端才能用它做重投去重。
// 为空时这里临时生成一个，保证消息体不为空（消费端会退化为只靠唯一索引去重）。
func (l *LikeMQ) Like(ctx context.Context, userID, videoID uint, eventID string) error {
	return l.publish(ctx, "like", likeLikeRK, userID, videoID, eventID)
}

func (l *LikeMQ) Unlike(ctx context.Context, userID, videoID uint, eventID string) error {
	return l.publish(ctx, "unlike", likeUnlikeRK, userID, videoID, eventID)
}

func (l *LikeMQ) publish(ctx context.Context, action, routingKey string, userID, videoID uint, eventID string) error {
	if l == nil || l.p == nil {
		return errors.New("like mq is not initialized")
	}
	if userID == 0 || videoID == 0 {
		return errors.New("userID and videoID are required")
	}
	if eventID == "" {
		id, err := newEventID(16)
		if err != nil {
			return err
		}
		eventID = id
	}
	event := LikeEvent{
		EventID:    eventID,
		Action:     action,
		UserID:     userID,
		VideoID:    videoID,
		OccurredAt: time.Now(),
	}
	return l.p.PublishJSON(ctx, likeExchange, routingKey, event)
}
