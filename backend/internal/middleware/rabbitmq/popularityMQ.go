package rabbitmq

import (
	"context"
	"errors"
	"time"
)

// PopularityMQ 负责「热度事件」的发布。
// 它和落库事件是两类消息、两个队列、两个 Worker：热度只影响 Redis 分钟桶与
// videos.popularity，落库失败不会阻塞热度，反之亦然。
type PopularityMQ struct {
	p *publisher
}

const (
	popularityExchange   = "video.popularity.events"
	popularityQueue      = "video.popularity.events"
	popularityBindingKey = "video.popularity.*"

	popularityUpdateRK = "video.popularity.update"
)

type PopularityEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	Change     int64     `json:"change"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewPopularityMQ(base *RabbitMQ) (*PopularityMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareTopic(ch, popularityExchange, popularityQueue, popularityBindingKey); err != nil {
		ch.Close()
		return nil, err
	}
	p, err := newPublisher(ch, DefaultConfirmTimeout)
	if err != nil {
		ch.Close()
		return nil, err
	}
	return &PopularityMQ{p: p}, nil
}

func (p *PopularityMQ) Close() error {
	if p == nil || p.p == nil {
		return nil
	}
	return p.p.Close()
}

// Update 发布一条热度事件。
//
// eventID 必须由调用方传入，不能在这里生成：发布失败时服务层会走降级直写 Redis，
// 而降级和消费端必须携带**同一个 event_id**，热度去重才能生效。
// 如果这里现生成一个，降级路径拿不到它，两条路径就会各加一遍热度。
//
// eventID 为空时这里临时生成一个，保证消息体字段不为空（消费端会因此降级为不去重）。
func (p *PopularityMQ) Update(ctx context.Context, videoID uint, change int64, eventID string) error {
	if p == nil || p.p == nil {
		return errors.New("popularity mq is not initialized")
	}
	if videoID == 0 || change == 0 {
		return errors.New("videoID and change are required")
	}
	if eventID == "" {
		id, err := newEventID(16)
		if err != nil {
			return err
		}
		eventID = id
	}
	event := PopularityEvent{
		EventID:    eventID,
		VideoID:    videoID,
		Change:     change,
		OccurredAt: time.Now().UTC(),
	}
	return p.p.PublishJSON(ctx, popularityExchange, popularityUpdateRK, event)
}
