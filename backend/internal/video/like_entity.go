package video

import "time"

// LikeEventRetention 是点赞事件去重台账的保留时长。
// 台账只服务于「同一条事件不重复生效」，远大于最长重投时间即可；
// 留 7 天是为了排查线上问题时还能回溯，而不是因为它需要这么久。
const LikeEventRetention = 7 * 24 * time.Hour

type Like struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	VideoID   uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"video_id"`
	AccountID uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"account_id"`
	CreatedAt time.Time `json:"created_at"`
}

// LikeEventLog 是点赞/取消点赞事件的去重台账。
//
// 只靠 likes 表的业务唯一键挡不住一种重投：like 处理成功但 ACK 丢失，用户随后
// 取消点赞（行被删除），旧 like 被重投时唯一索引不再冲突，于是点赞被「复活」。
// event_id 在发布前生成、重投时保持不变，用它做幂等键才能把「同一条事件只生效
// 一次」落到 like 和 unlike 两侧。
//
// 去重记录与业务写入必须同事务：先占位再执行，事务回滚时占位一起回滚，
// 不会出现「标记了处理过、但业务没执行」的永久丢失。
type LikeEventLog struct {
	ID        uint      `gorm:"primaryKey"`
	EventID   string    `gorm:"uniqueIndex:idx_like_event_id;size:64;not null"`
	VideoID   uint      `gorm:"index:idx_like_event_video_account,priority:1"`
	AccountID uint      `gorm:"index:idx_like_event_video_account,priority:2"`
	Action    string    `gorm:"type:varchar(16);not null"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (LikeEventLog) TableName() string { return "like_events" }

type LikeRequest struct {
	VideoID uint `json:"video_id"`
}
