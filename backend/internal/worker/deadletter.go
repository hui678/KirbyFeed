package worker

import "time"

// DeadLetter 是死信队列的落库记录。
//
// 死信队列此前只打日志：日志会被轮转、不能按 event_id 查询，也无法统计「最近
// 有多少消息在进死信」。落一张表之后，排查（事件体、重试轮数、失败原因）和
// 人工重放都有了稳定入口；重放仍然是人工操作，因为自动重放一条「有毒」消息
// 只会让它再次进入死信。
//
// (queue, event_id) 唯一：DLQ 消费者在「落库成功、ACK 前崩溃」时会被重投，
// 用 INSERT IGNORE 避免产生重复记录。event_id 为空的历史消息落 NULL，
// MySQL 唯一索引允许多个 NULL，不会互相冲突。
type DeadLetter struct {
	ID         uint      `gorm:"primaryKey"`
	Queue      string    `gorm:"size:128;not null;uniqueIndex:idx_dead_letter_queue_event,priority:1"`
	EventID    *string   `gorm:"size:64;uniqueIndex:idx_dead_letter_queue_event,priority:2"`
	Exchange   string    `gorm:"size:128"`
	RoutingKey string    `gorm:"size:128"`
	RetryCount int       `gorm:"not null;default:0"`
	Reason     string    `gorm:"size:255"`
	Body       []byte    `gorm:"type:longblob"`
	CreatedAt  time.Time `gorm:"autoCreateTime"`
}

func (DeadLetter) TableName() string { return "dead_letters" }
