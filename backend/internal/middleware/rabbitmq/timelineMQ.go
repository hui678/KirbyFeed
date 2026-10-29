package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrBrokerUnavailable 表示故障发生在「连接 / broker」层面：channel 已关闭、
// 发布 I/O 失败、confirm 超时等，与具体某条消息无关。
// Outbox poller 对这类错误**不消耗重试额度**：MQ 整体不可用期间消息应该留在
// pending 等恢复，而不是每失败一次烧一次额度、几十秒后被打成终态 failed。
var ErrBrokerUnavailable = errors.New("rabbitmq broker unavailable")

// ErrPublishRejected 表示 broker 明确 NACK 了这条消息，属于消息级失败。
var ErrPublishRejected = errors.New("rabbitmq rejected the message")

// TimelineMQ 是时间线事件的**发布端**封装。
//
// 开启 publisher confirm 模式后，broker 必须对每条消息回 ACK/NACK。
// 发布使用 PublishWithDeferredConfirm：每条消息拿到自己的 DeferredConfirmation，
// 按 DeliveryTag 精确等待自己的确认，不存在「取下一条确认」的错位问题；
// 超时的消息即使 ACK 迟到，也只会落在自己的 DeferredConfirmation 上。
type TimelineMQ struct {
	ch *amqp.Channel
}

const (
	timelineExchange   = "video.timeline.events"
	timelineQueue      = "video.timeline.update.queue"
	timelineBindingKey = "video.timeline.*"
	timelinePublishRK  = "video.timeline.publish"

	// confirmTimeout 等待 broker 确认的上限。超时按失败处理，
	// 由 Outbox 的退避重试兜底；是否重建 channel 由调用方决定。
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
	// false = 非阻塞，Confirm 调用本身不等 broker 回应。
	// 必须开启 confirm 模式，PublishWithDeferredConfirm 才会返回非 nil 的确认对象。
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}
	if err := DeclareTimelineTopic(ch); err != nil {
		ch.Close()
		return nil, err
	}
	return &TimelineMQ{ch: ch}, nil
}

// Close 关闭发布通道。
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
		return fmt.Errorf("%w: timeline mq is not initialized", ErrBrokerUnavailable)
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
	body, err := json.Marshal(timeline)
	if err != nil {
		return err
	}

	dc, err := t.ch.PublishWithDeferredConfirm(
		timelineExchange,
		timelinePublishRK,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("%w: publish: %v", ErrBrokerUnavailable, err)
	}
	if dc == nil {
		return fmt.Errorf("%w: publisher confirm is not enabled", ErrBrokerUnavailable)
	}

	waitCtx, cancel := context.WithTimeout(ctx, confirmTimeout)
	defer cancel()

	acked, err := dc.WaitContext(waitCtx)
	if err != nil {
		return fmt.Errorf("%w: confirm timeout: %v", ErrBrokerUnavailable, err)
	}
	if !acked {
		// channel 关闭时库会把所有未决确认置为 nack，这种是基础设施故障；
		// 只有 channel 仍存活时的 nack 才是 broker 明确拒收这条消息。
		if t.ch.IsClosed() {
			return fmt.Errorf("%w: channel closed before confirm", ErrBrokerUnavailable)
		}
		return fmt.Errorf("%w: delivery tag %d", ErrPublishRejected, dc.DeliveryTag)
	}
	return nil
}
