package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TimelineMQ 是时间线事件的**发布端**封装。
//
// 关键点：它开启 RabbitMQ 的 publisher confirm 模式。开启后 broker 必须对每条消息
// 回一个 ACK/NACK，PublishVideo 会一直等到这个确认才返回。
// 这样调用方（Outbox poller）只有在"broker 真的收下了消息"之后才能把本地消息置为 sent，
// 否则"发出去但没落盘 + 本地消息已删"就会造成时间线永久缺一条数据。
type TimelineMQ struct {
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
}

const (
	timelineExchange   = "video.timeline.events"
	timelineQueue      = "video.timeline.update.queue"
	timelineBindingKey = "video.timeline.*"
	timelinePublishRK  = "video.timeline.publish"

	// confirmTimeout 等待 broker 确认的上限。超时按失败处理，
	// 由 Outbox 的退避重试兜底，调用方会重建 channel 避免迟到的确认错位。
	confirmTimeout = 5 * time.Second
)

// TimelineEvent 是时间线事件的载荷。
// 注意：发布端与消费端（worker/outboxworker.go）共用这一个结构体，
// 改字段等于改契约，两边要一起改。
type TimelineEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	CreateTime int64     `json:"create_time"`
	OccurredAt time.Time `json:"occurred_at"`
}

// DeclareTimelineTopic 声明时间线的交换机/队列/绑定，供消费者侧确保拓扑存在（幂等）。
func DeclareTimelineTopic(ch *amqp.Channel) error {
	return DeclareTopic(ch, timelineExchange, timelineQueue, timelineBindingKey)
}

func NewTimelineMQ(base *RabbitMQ) (*TimelineMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	// false = 非阻塞，Confirm 调用本身不等 broker 回应
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}
	// 必须在 Confirm 之后注册；缓冲区 1 就够，因为 PublishVideo 是串行调用并立即取走确认
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := DeclareTimelineTopic(ch); err != nil {
		ch.Close()
		return nil, err
	}
	return &TimelineMQ{ch: ch, confirms: confirms}, nil
}

// Close 关闭发布通道（同时关闭 confirm 通知通道）。
func (t *TimelineMQ) Close() error {
	if t == nil || t.ch == nil {
		return nil
	}
	return t.ch.Close()
}

// PublishVideo 发布一条时间线事件。
//
// eventID 必须由调用方（Outbox poller）从 outbox 行里带出来，而不是在这里生成：
// confirm 丢失时同一条 outbox 消息会被重投，如果每次发布都新生成一个 ID，
// 同一条业务事件前后两次投递带的 event_id 不一样，消费端拿它去重就完全失效。
//
// eventID 为空时这里会临时生成一个，保证消息体字段不为空（消费端会因此降级为不去重）。
func (t *TimelineMQ) PublishVideo(ctx context.Context, videoID uint, createTime time.Time, eventID string) error {
	if t == nil || t.ch == nil {
		return errors.New("timeline mq is not initialized")
	}
	if videoID == 0 {
		return errors.New("videoID are required")
	}
	if eventID == "" {
		id, err := newEventID(16)
		if err != nil {
			return err
		}
		eventID = id
	}
	timeline := TimelineEvent{
		EventID:    eventID,
		VideoID:    videoID,
		CreateTime: createTime.UnixMilli(),
		OccurredAt: time.Now(),
	}
	if err := PublishJSON(ctx, t.ch, timelineExchange, timelinePublishRK, timeline); err != nil {
		return err
	}
	return t.waitConfirm(ctx)
}

// waitConfirm 等待 broker 对本条消息的确认。
//
// 前提：PublishVideo 不会被多个 goroutine 并发调用（Outbox poller 是串行循环），
// 所以这里直接"取下一条确认"即可，不需要维护 DeliveryTag 映射。
// 一旦超时/NACK，调用方应当重建 channel，否则迟到的确认会被下一条消息误当成自己的 ACK。
func (t *TimelineMQ) waitConfirm(ctx context.Context) error {
	if t == nil || t.confirms == nil {
		return errors.New("publisher confirm is not enabled")
	}
	timer := time.NewTimer(confirmTimeout)
	defer timer.Stop()

	select {
	case c, ok := <-t.confirms:
		if !ok {
			return errors.New("publisher confirm channel closed")
		}
		if !c.Ack {
			return errors.New("broker nacked the timeline message")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("publisher confirm timeout")
	}
}
