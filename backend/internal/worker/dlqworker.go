package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"feedsystem_video_go/internal/middleware/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DLQWorker 消费死信队列。
//
// 死信队列是「最终一致」链路里的兜底观察点而不是垃圾桶：能走到这里的消息说明
// 已经经历了「进程内 3 次退避重试 + MaxRetryCount 轮延迟重投」全部失败。
// 这里的职责是把它记录下来（生产环境应换成告警 / 落告警表 / 人工重投工具），
// 然后 Ack —— 因为继续 requeue 只会让一条注定失败的消息反复冲击数据库。
type DLQWorker struct {
	queue string
}

// NewDLQWorker 传入的是**主队列名**，死信队列名由 rabbitmq.DLQQueueName 推导，
// 保证与 DeclareDLX 声明的队列一致。
func NewDLQWorker(mainQueue string) *DLQWorker {
	return &DLQWorker{queue: rabbitmq.DLQQueueName(mainQueue)}
}

func (w *DLQWorker) Run(ctx context.Context, ch *amqp.Channel) error {
	if w == nil || w.queue == "" {
		return errors.New("dlq worker is not initialized")
	}
	if ch == nil {
		return errors.New("channel is not initialized")
	}
	deliveries, err := ch.Consume(w.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Printf("DLQWorker started, consuming %s", w.queue)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			w.handle(d)
		}
	}
}

func (w *DLQWorker) handle(d amqp.Delivery) {
	var fields map[string]json.RawMessage
	eventID := ""
	if err := json.Unmarshal(d.Body, &fields); err == nil {
		if raw, ok := fields["event_id"]; ok {
			_ = json.Unmarshal(raw, &eventID)
		}
	}
	// TODO(生产化)：这里应该接入告警（如企业微信 / Prometheus counter），
	// 并提供一个按 event_id 重投的管理接口，而不是只在日志里打印。
	log.Printf("[DLQ] queue=%s event_id=%s retryCount=%d body=%s",
		w.queue, eventID, rabbitmq.GetRetryCount(d), string(d.Body))
	_ = d.Ack(false)
}
