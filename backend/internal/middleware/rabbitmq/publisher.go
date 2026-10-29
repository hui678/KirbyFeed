package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/observability"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DefaultConfirmTimeout 是等待 broker 返回 publisher confirm 的默认上限。
// 超过这个时间就认为「发布结果未知」，调用方应走降级直写，而不是一直等下去。
const DefaultConfirmTimeout = 3 * time.Second

// publisher 在一个开启了 publisher confirm 的 Channel 上发布消息，并同步等待 broker 确认。
//
// 为什么要串行（mu）：
//   - confirm 是按 Channel 维度、按发布顺序返回的，没有携带 deliveryTag 之外的强归属信息；
//   - 同一个 Channel 被多个 HTTP 请求并发使用时，两个 goroutine 会互相取走对方的确认，
//     导致「我的消息其实被 nack 了，却读到了别人的 ack」；
//   - 因此把「发布 + 等确认」用一把锁包成原子操作，代价是该 Channel 上的发布串行化。
//
// 代价说明：串行会让单 Channel 的发布吞吐受限于一次 RTT。对点赞/评论这类写接口，
// 一次请求只发 1~2 条消息，瓶颈不在 MQ；真需要更高吞吐时应该按并发度开多个 publisher，
// 而不是取消锁。
type publisher struct {
	mu       sync.Mutex
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
	timeout  time.Duration
}

// newPublisher 把传入的 Channel 切换到 confirm 模式并注册确认回调。
func newPublisher(ch *amqp.Channel, timeout time.Duration) (*publisher, error) {
	if ch == nil {
		return nil, errors.New("channel is not initialized")
	}
	if timeout <= 0 {
		timeout = DefaultConfirmTimeout
	}
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	// 缓冲 1 条即可：每次发布后都会立刻把确认取走，不会堆积。
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return &publisher{ch: ch, confirms: confirms, timeout: timeout}, nil
}

// Channel 返回底层 Channel，供声明拓扑等场景复用。
func (p *publisher) Channel() *amqp.Channel {
	if p == nil {
		return nil
	}
	return p.ch
}

func (p *publisher) Close() error {
	if p == nil || p.ch == nil {
		return nil
	}
	return p.ch.Close()
}

// PublishJSON 序列化后发布，并等待 broker 的 confirm。
// 返回 nil 只表示「broker 已确认收下（持久化模式下已落盘）」，不代表消费者已处理。
func (p *publisher) PublishJSON(ctx context.Context, exchange, routingKey string, payload any) error {
	if p == nil || p.ch == nil {
		return errors.New("publisher is not initialized")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return p.publish(ctx, exchange, routingKey, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         b,
	})
}

// PublishRaw 直接发布字节流（用于重试：原样搬运 Body，不重新序列化）。
func (p *publisher) PublishRaw(ctx context.Context, exchange, routingKey string, body []byte, contentType string, headers amqp.Table) error {
	if p == nil || p.ch == nil {
		return errors.New("publisher is not initialized")
	}
	if contentType == "" {
		contentType = "application/json"
	}
	return p.publish(ctx, exchange, routingKey, amqp.Publishing{
		ContentType:  contentType,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         body,
	})
}

// publish 是两种发布方式的公共实现：校验 → 加锁 → 发布 → 等确认 → 记指标。
// 指标按 exchange + 结果打标签，用来回答「发布失败率是否在升高」。
func (p *publisher) publish(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	if exchange == "" || routingKey == "" {
		return errors.New("exchange and routingKey are required")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg); err != nil {
		observability.Inc("feedsystem_publish_total", "exchange", exchange, "result", "failure")
		return err
	}
	if err := p.waitConfirm(ctx); err != nil {
		observability.Inc("feedsystem_publish_total", "exchange", exchange, "result", "failure")
		return err
	}
	observability.Inc("feedsystem_publish_total", "exchange", exchange, "result", "success")
	return nil
}

func (p *publisher) waitConfirm(ctx context.Context) error {
	timer := time.NewTimer(p.timeout)
	defer timer.Stop()

	select {
	case c, ok := <-p.confirms:
		if !ok {
			return errors.New("publisher confirm channel closed")
		}
		if !c.Ack {
			return errors.New("broker nacked the message")
		}
		return nil
	case <-timer.C:
		// 超时后那条 confirm 可能稍后才到，必须清掉，否则会被下一次发布误读成自己的确认。
		select {
		case <-p.confirms:
		default:
		}
		return errors.New("publisher confirm timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}
