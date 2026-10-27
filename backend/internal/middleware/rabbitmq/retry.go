package rabbitmq

import (
	"context"
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// RetryExchange 是所有「延迟重投」消息共用的 topic 交换机。
	RetryExchange = "retry.events"
	// DefaultRetryTTL 是消息在重试队列里等待的时长。
	// TTL 到期后由 RabbitMQ 自己把消息死信回主交换机，不需要额外的定时任务。
	DefaultRetryTTL = 30 * time.Second

	retryQueueSuffix = ".retry"
)

// RetryQueueName / RetryRoutingKey：约定重试队列名与绑定键同名，便于按队列名推导。
func RetryQueueName(mainQueue string) string  { return mainQueue + retryQueueSuffix }
func RetryRoutingKey(mainQueue string) string { return mainQueue + retryQueueSuffix }

// DeclareRetry 声明某个主队列的「延迟重投」拓扑：
//
//	retry.events(topic) --"<mainQueue>.retry"--> <mainQueue>.retry
//	    <mainQueue>.retry: x-message-ttl=ttl, x-dead-letter-exchange=<mainExchange>
//
// 关键点：重试队列**故意不设** x-dead-letter-routing-key。
// RabbitMQ 的规则是「没指定死信路由键时沿用消息原始 routing key」，
// 于是 TTL 到期后消息会带着原始的 `like.like` / `comment.publish` 回到主交换机，
// 再被主队列的绑定键接住，天然完成一次重投，且不需要重新计算路由键。
func DeclareRetry(ch *amqp.Channel, mainExchange, mainQueue string, ttl time.Duration) error {
	if ch == nil {
		return errors.New("channel is not initialized")
	}
	if mainExchange == "" || mainQueue == "" {
		return errors.New("mainExchange/mainQueue is required")
	}
	if ttl <= 0 {
		ttl = DefaultRetryTTL
	}

	if err := ch.ExchangeDeclare(
		RetryExchange, "topic", true, false, false, false, nil,
	); err != nil {
		return err
	}

	retryQueue := RetryQueueName(mainQueue)
	if _, err := ch.QueueDeclare(
		retryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":          int64(ttl.Milliseconds()),
			"x-dead-letter-exchange": mainExchange,
			// 不设 x-dead-letter-routing-key，保留原始 routing key
		},
	); err != nil {
		return err
	}
	if err := ch.QueueBind(retryQueue, RetryRoutingKey(mainQueue), RetryExchange, false, nil); err != nil {
		return err
	}
	log.Printf("retry topology ready: queue=%s ttl=%v -> %s", retryQueue, ttl, mainExchange)
	return nil
}

// RetryRouter 把「进程内重试已经耗尽」的消息投递到重试队列，交给 TTL 延迟重投。
// 它是「消费端重试」和「MQ 拓扑」之间唯一的耦合点，worker 只依赖这个接口。
type RetryRouter struct {
	p         *publisher
	mainQueue string
}

// RetryPublisher 是 worker 侧依赖的抽象，便于测试与替换。
type RetryPublisher interface {
	PublishRetry(ctx context.Context, d amqp.Delivery, reason string) error
}

// NewRetryRouter 在独立 Channel 上开启 confirm 并声明重试拓扑。
func NewRetryRouter(base *RabbitMQ, mainExchange, mainQueue string, ttl time.Duration) (*RetryRouter, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareRetry(ch, mainExchange, mainQueue, ttl); err != nil {
		ch.Close()
		return nil, err
	}
	p, err := newPublisher(ch, DefaultConfirmTimeout)
	if err != nil {
		ch.Close()
		return nil, err
	}
	return &RetryRouter{p: p, mainQueue: mainQueue}, nil
}

func (r *RetryRouter) Close() error {
	if r == nil || r.p == nil {
		return nil
	}
	return r.p.Close()
}

// PublishRetry 把原始消息体重新投递到重试队列。
// 只搬运 Body，不搬运 x-death：x-death 由 RabbitMQ 在每次死信时自行累加，
// 手抄过去会让重试计数失真。
func (r *RetryRouter) PublishRetry(ctx context.Context, d amqp.Delivery, reason string) error {
	if r == nil || r.p == nil {
		return errors.New("retry router is not initialized")
	}
	headers := amqp.Table{
		"retry-original-queue": r.mainQueue,
	}
	if reason != "" {
		headers["retry-reason"] = reason
	}
	contentType := d.ContentType
	if contentType == "" {
		contentType = "application/json"
	}
	return r.p.PublishRaw(ctx, RetryExchange, RetryRoutingKey(r.mainQueue), d.Body, contentType, headers)
}
