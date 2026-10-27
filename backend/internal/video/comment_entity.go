package video

import "time"

type Comment struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Username  string    `gorm:"index" json:"username"`
	VideoID   uint      `gorm:"index" json:"video_id"`
	AuthorID  uint      `gorm:"index" json:"author_id"`
	Content   string    `gorm:"type:text" json:"content"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	// EventID 是消费端幂等键：同一条评论事件（无论走 MQ 还是降级直写）携带同一个
	// event_id，靠唯一索引 idx_comment_event_id 去重，重复投递只会插入一次。
	//
	// 用指针而不是 string：AutoMigrate 加列时历史行的值是 NULL，而 MySQL 唯一索引
	// 允许多个 NULL；如果直接用 string，历史行会全部落空串 '' 并立刻违反唯一约束，
	// 导致建索引失败、服务起不来。
	EventID *string `gorm:"uniqueIndex:idx_comment_event_id;size:64" json:"event_id,omitempty"`
}

type PublishCommentRequest struct {
	VideoID uint   `json:"video_id"`
	Content string `json:"content"`
}

type DeleteCommentRequest struct {
	CommentID uint `json:"comment_id"`
}

type GetAllCommentsRequest struct {
	VideoID uint `json:"video_id"`
}
