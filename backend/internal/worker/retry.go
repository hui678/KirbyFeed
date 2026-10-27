package worker

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// maxLocalRetries 是「进程内」的重试次数：这条消息在当前 worker 里最多被处理
	// 1 + 3 次。用于抹平瞬时故障（网络抖动、锁等待、连接闪断）。
	maxLocalRetries = 3
	baseBackoff     = time.Second
	maxBackoff      = 8 * time.Second
)

// RetryPublisher 把「进程内重试已耗尽」的消息交给重试队列延迟重投。
// 由 rabbitmq.RetryRouter 实现，worker 只依赖接口，方便单测替换。
type RetryPublisher interface {
	PublishRetry(ctx context.Context, d amqp.Delivery, reason string) error
}

// Consumer 是所有 worker 共用的消费骨架，把「重试 / 退避 / 重投 / 死信」这四件
// 容易写错的事集中到一处，业务 worker 只需要提供一个 process 函数。
type Consumer struct {
	Name    string
	Queue   string
	Retry   RetryPublisher
	Process func(ctx context.Context, d amqp.Delivery) error
}

// Run 消费一个队列直到 ctx 结束或通道关闭。
func (c *Consumer) Run(ctx context.Context, ch *amqp.Channel) error {
	if c == nil || c.Process == nil {
		return errors.New("consumer is not initialized")
	}
	if ch == nil {
		return errors.New("channel is not initialized")
	}
	deliveries, err := ch.Consume(c.Queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			c.handle(ctx, d)
		}
	}
}

// handle 三级容错：
//  1. 进程内指数退避重试（带 jitter），抹平瞬时故障；
//  2. 仍失败 → 投递到重试队列，由 TTL 延迟重投，最多 MaxRetryCount 轮；
//  3. 超过轮数上限 → Nack(requeue=false) 进死信队列，等待人工/告警处理。
//
// 关键：任何时候都不 Ack 掉一条没处理成功的消息，这是「不静默丢消息」的底线。
func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	attempts := rabbitmq.GetRetryCount(d)

	var lastErr error
	for i := 0; i <= maxLocalRetries; i++ {
		select {
		case <-ctx.Done():
			// 进程要退出了，把消息退回队列，交给下一个消费者或下次启动。
			_ = d.Nack(false, true)
			return
		default:
		}

		err := c.Process(ctx, d)
		if err == nil {
			_ = d.Ack(false)
			return
		}
		lastErr = err

		if i == maxLocalRetries {
			break
		}
		wait := backoff(i)
		log.Printf("%s: 处理失败，%v 后重试 (%d/%d): %v", c.Name, wait, i+1, maxLocalRetries, err)
		if !sleep(ctx, wait) {
			_ = d.Nack(false, true)
			return
		}
	}

	// 进程内重试耗尽：优先交给重试队列做延迟重投，而不是立刻判死。
	if c.Retry != nil && attempts < rabbitmq.MaxRetryCount {
		if err := c.Retry.PublishRetry(ctx, d, lastErr.Error()); err == nil {
			log.Printf("%s: 进程内重试耗尽，转入重试队列 (第 %d 轮): %v", c.Name, attempts+1, lastErr)
			_ = d.Ack(false)
			return
		} else {
			log.Printf("%s: 投递重试队列失败，直接进死信: %v", c.Name, err)
		}
	}

	log.Printf("%s: 重试 %d 轮仍失败，进入死信队列 %s: %v",
		c.Name, attempts, rabbitmq.DLQQueueName(c.Queue), lastErr)
	// requeue=false：消息走主队列的 x-dead-letter-exchange 进入专属死信队列。
	_ = d.Nack(false, false)
}

// backoff 指数退避 + 抖动。抖动是为了避免同一批失败消息在同一秒集体重试，
// 把下游（MySQL/Redis）再打一次尖峰。
func backoff(attempt int) time.Duration {
	wait := baseBackoff * time.Duration(1<<uint(attempt))
	if wait > maxBackoff {
		wait = maxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(wait / 2)))
	return wait/2 + jitter
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
