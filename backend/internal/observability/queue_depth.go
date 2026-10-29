package observability

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SampleQueueDepth 周期性读取队列积压数，写进 feedsystem_queue_depth{queue}。
//
// 用独立的 Channel + QueueInspect（AMQP 的 queue.declare passive），不复用
// 消费者/发布者的 Channel：inspect 失败或变慢不能影响业务链路。
// 这是「积压」在应用侧唯一的低门槛观测方式；更完整的指标（unacked、消费速率）
// 应交给 RabbitMQ 自带的 Prometheus exporter。
func SampleQueueDepth(ctx context.Context, conn *amqp.Connection, queues []string, interval time.Duration) {
	if conn == nil || len(queues) == 0 {
		return
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		sampleQueueDepth(conn, queues)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sampleQueueDepth(conn *amqp.Connection, queues []string) {
	ch, err := conn.Channel()
	if err != nil {
		return // 连接暂时不可用：保持上一次的 gauge，不要在故障时把它刷成 0
	}
	defer ch.Close()

	for _, q := range queues {
		info, err := ch.QueueInspect(q)
		if err != nil {
			continue
		}
		SetGauge("feedsystem_queue_depth", int64(info.Messages), "queue", q)
	}
}
