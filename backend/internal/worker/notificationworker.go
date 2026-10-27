package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Notification struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	RecipientID uint      `gorm:"index;not null" json:"recipient_id"`
	SenderID    uint      `gorm:"not null" json:"sender_id"`
	Type        string    `gorm:"type:varchar(50);not null" json:"type"`
	TargetID    uint      `json:"target_id"`
	Content     string    `gorm:"type:varchar(255)" json:"content"`
	IsRead      bool      `gorm:"default:false" json:"is_read"`
	CreatedAt   time.Time `gorm:"autoCreateTime" json:"created_at"`
	// EventID 是幂等键：通知队列与业务队列绑定在同一个交换机上，消息重投、
	// 或者 worker 重启后的补投，都会让同一条事件再走一遍。靠唯一索引去重，
	// 用户才不会收到两条一模一样的「xxx 点赞了你的视频」。
	// 用指针是为了让历史行的值为 NULL（MySQL 唯一索引允许多个 NULL），
	// 否则 AutoMigrate 建索引时会被历史空串直接撞上唯一约束。
	EventID *string `gorm:"uniqueIndex:idx_notification_event_id;size:64" json:"event_id,omitempty"`
}

type NotificationWorker struct {
	db    *gorm.DB
	queue string
	hub   NotificationHub
	retry RetryPublisher
}

type NotificationHub interface {
	Push(userID uint, n *Notification)
}

func NewNotificationWorker(db *gorm.DB, queue string, hub NotificationHub, retry RetryPublisher) *NotificationWorker {
	return &NotificationWorker{db: db, queue: queue, hub: hub, retry: retry}
}

func (w *NotificationWorker) Run(ctx context.Context, ch *amqp.Channel) error {
	if w == nil || w.db == nil {
		return errors.New("notification worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}
	if err := w.db.WithContext(ctx).AutoMigrate(&Notification{}); err != nil {
		return err
	}
	c := &Consumer{
		Name:    "NotificationWorker",
		Queue:   w.queue,
		Retry:   w.retry,
		Process: w.process,
	}
	return c.Run(ctx, ch)
}

func (w *NotificationWorker) process(ctx context.Context, d amqp.Delivery) error {
	body := d.Body
	if len(body) == 0 {
		return nil
	}
	routingKey := d.RoutingKey

	var notif *Notification
	var eventID string

	switch {
	case routingKey == "like.like":
		var evt rabbitmq.LikeEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			return nil
		}
		if evt.UserID == 0 || evt.VideoID == 0 {
			return nil
		}
		eventID = evt.EventID
		var authorID uint
		if err := w.db.WithContext(ctx).Model(&struct {
			ID       uint
			AuthorID uint
		}{}).Table("videos").Where("id = ?", evt.VideoID).Select("author_id").Scan(&authorID).Error; err != nil {
			return err
		}
		if authorID == 0 || authorID == evt.UserID {
			return nil
		}
		notif = &Notification{RecipientID: authorID, SenderID: evt.UserID, Type: "like", TargetID: evt.VideoID, Content: "点赞了你的视频"}

	case routingKey == "comment.publish":
		var evt rabbitmq.CommentEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			return nil
		}
		if evt.AuthorID == 0 || evt.VideoID == 0 {
			return nil
		}
		eventID = evt.EventID
		var authorID uint
		if err := w.db.WithContext(ctx).Model(&struct {
			ID       uint
			AuthorID uint
		}{}).Table("videos").Where("id = ?", evt.VideoID).Select("author_id").Scan(&authorID).Error; err != nil {
			return err
		}
		if authorID == 0 || authorID == evt.AuthorID {
			return nil
		}
		notif = &Notification{RecipientID: authorID, SenderID: evt.AuthorID, Type: "comment", TargetID: evt.VideoID, Content: "评论了你的视频"}

	case routingKey == "social.follow":
		var evt rabbitmq.SocialEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			return nil
		}
		if evt.FollowerID == 0 || evt.VloggerID == 0 {
			return nil
		}
		eventID = evt.EventID
		notif = &Notification{RecipientID: evt.VloggerID, SenderID: evt.FollowerID, Type: "follow", TargetID: evt.FollowerID, Content: "关注了你"}
	}

	if notif == nil {
		return nil
	}
	if eid := strings.TrimSpace(eventID); eid != "" {
		notif.EventID = &eid
	}

	// 幂等插入：同一条事件重复投递时 RowsAffected=0，直接返回，
	// 不再重复推送 SSE（否则用户会看到两条一样的通知）。
	res := w.db.WithContext(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).Create(notif)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return nil
	}
	if w.hub != nil {
		w.hub.Push(notif.RecipientID, notif)
	}
	return nil
}
