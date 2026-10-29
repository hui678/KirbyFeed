package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/observability"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DLQWorker 消费死信队列。
//
// 死信队列是「最终一致」链路里的兜底观察点而不是垃圾桶：能走到这里的消息说明
// 已经经历了「进程内 3 次退避重试 + MaxRetryCount 轮延迟重投」全部失败。
// 这里的职责是把它落表（event_id / 重试轮数 / 原因 / 原始 body）并打日志，
// 然后 Ack —— 因为继续 requeue 只会让一条注定失败的消息反复冲击数据库。
//
// 落表之后可以按 event_id 查询、统计死信速率，并在修复根因后人工重放；
// 重放接口不在这里自动做：自动重放有毒消息只会形成「进死信 → 重放 → 再进死信」的循环。
type DLQWorker struct {
	db    *gorm.DB
	queue string
}

// NewDLQWorker 传入的是**主队列名**，死信队列名由 rabbitmq.DLQQueueName 推导，
// 保证与 DeclareDLX 声明的队列一致。db 为 nil 时退化为只打日志。
func NewDLQWorker(db *gorm.DB, mainQueue string) *DLQWorker {
	return &DLQWorker{db: db, queue: rabbitmq.DLQQueueName(mainQueue)}
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
			w.handle(ctx, d)
		}
	}
}

func (w *DLQWorker) handle(ctx context.Context, d amqp.Delivery) {
	eventID := extractEventID(d.Body)
	reason := headerString(d.Headers, "retry-reason")
	retryCount := rabbitmq.GetRetryCount(d)

	observability.Inc("feedsystem_dlq_consumed_total", "queue", w.queue)

	if w.db != nil {
		rec := &DeadLetter{
			Queue:      w.queue,
			Exchange:   d.Exchange,
			RoutingKey: d.RoutingKey,
			RetryCount: retryCount,
			Reason:     truncateRunes(reason, 255),
			Body:       d.Body,
		}
		if eventID != "" {
			rec.EventID = &eventID
		}
		if err := w.persist(ctx, rec); err != nil {
			// 落表失败说明数据库也不可用。此时仍然 ACK：消息已经在死信队列里
			// 走完了所有重试，继续 requeue 只会热循环。日志 + 指标负责让它可见。
			log.Printf("[DLQ] 落表失败 queue=%s event_id=%s: %v", w.queue, eventID, err)
			observability.Inc("feedsystem_dlq_persist_total", "queue", w.queue, "result", "failure")
		} else {
			observability.Inc("feedsystem_dlq_persist_total", "queue", w.queue, "result", "success")
		}
	}

	log.Printf("[DLQ] queue=%s event_id=%s retryCount=%d reason=%s body=%s",
		w.queue, eventID, retryCount, reason, string(d.Body))
	_ = d.Ack(false)
}

// persist 最多尝试 3 次。数据库抖动通常在一秒内恢复；超过就放弃并交给日志/指标，
// 避免 DLQ 消费者被一条记录卡住、拖住后面所有死信。
func (w *DLQWorker) persist(ctx context.Context, rec *DeadLetter) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 && !sleepBriefly(ctx, 200*time.Millisecond) {
			return lastErr
		}
		lastErr = w.db.WithContext(ctx).
			Clauses(clause.Insert{Modifier: "IGNORE"}).
			Create(rec).Error
		if lastErr == nil {
			return nil
		}
	}
	return lastErr
}

func extractEventID(body []byte) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return ""
	}
	raw, ok := fields["event_id"]
	if !ok {
		return ""
	}
	var eventID string
	if err := json.Unmarshal(raw, &eventID); err != nil {
		return ""
	}
	return eventID
}

func headerString(headers amqp.Table, key string) string {
	v, ok := headers[key]
	if !ok {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func sleepBriefly(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
