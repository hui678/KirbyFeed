package video

import "time"

type Video struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	AuthorID    uint      `gorm:"index;not null" json:"author_id"`
	Username    string    `gorm:"type:varchar(255);not null" json:"username"`
	Title       string    `gorm:"type:varchar(255);not null" json:"title"`
	Description string    `gorm:"type:varchar(255);" json:"description,omitempty"`
	PlayURL     string    `gorm:"type:varchar(255);not null" json:"play_url"`
	CoverURL    string    `gorm:"type:varchar(255);not null" json:"cover_url"`
	CreateTime  time.Time `gorm:"autoCreateTime;index:idx_videos_create_time,sort:desc;index:idx_videos_popularity_time_id,priority:2,sort:desc" json:"create_time"`
	LikesCount  int64     `gorm:"column:likes_count;not null;default:0;index:idx_videos_likes_count_id,priority:1,sort:desc" json:"likes_count"`
	Popularity  int64     `gorm:"column:popularity;not null;default:0;index:idx_videos_popularity_time_id,priority:1,sort:desc" json:"popularity"`
}

type PublishVideoRequest struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	PlayURL     string `json:"play_url"`
	CoverURL    string `json:"cover_url"`
}

type DeleteVideoRequest struct {
	ID uint `json:"id"`
}

type ListByAuthorIDRequest struct {
	AuthorID uint `json:"author_id"`
}

type GetDetailRequest struct {
	ID uint `json:"id"`
}

type UpdateLikesCountRequest struct {
	ID         uint  `json:"id"`
	LikesCount int64 `json:"likes_count"`
}

// Outbox 状态机：
//
//	pending ──抢占(CAS)──> processing ──broker 确认──> sent ──保留24h后清理──> (删除)
//	   ^                       │
//	   │                       └──投递失败──> 重试额度未用完: 回 pending(retry_count+1, 指数退避)
//	   │                                     重试额度已用完: failed(终态, 等人工介入)
//	   └────────────── claimed_at 超时未确认: 进程崩溃回收 ─────────────┘
const (
	OutboxStatusPending    = "pending"
	OutboxStatusProcessing = "processing"
	OutboxStatusSent       = "sent"
	OutboxStatusFailed     = "failed"
)

const (
	// OutboxMaxRetries 单条消息的最大重试次数，超过则转 failed，避免无限重投
	OutboxMaxRetries = 5
	// OutboxBaseBackoff 退避基数：第 n 次重试等待 OutboxBaseBackoff * 2^(n-1)
	OutboxBaseBackoff = 2 * time.Second
	// OutboxClaimTimeout processing 状态超过这个时长还没落地，说明抢占它的进程挂了，可以被回收
	OutboxClaimTimeout = 60 * time.Second
	// OutboxSentRetention sent 状态的消息保留时长，之后由 PurgeSent 清理
	OutboxSentRetention = 24 * time.Hour
)

// OutboxBackoff 计算第 retryCount 次重试前需要等待的时间（指数退避）
func OutboxBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 0
	}
	shift := retryCount - 1
	if shift > 10 {
		shift = 10
	}
	return OutboxBaseBackoff * time.Duration(1<<uint(shift))
}

type OutboxMsg struct {
	ID          uint       `gorm:"primaryKey"`
	VideoID     uint       `gorm:"index"`
	EventType   string     `gorm:"type:varchar(50)"`
	CreateTime  time.Time  `gorm:"autoCreateTime"`
	Status      string     `gorm:"type:varchar(50);index;default:pending"`
	RetryCount  int        `gorm:"not null;default:0"`
	NextRetryAt *time.Time `gorm:"index"` // 退避期间不可见，避免故障时的忙轮询
	ClaimedAt   *time.Time // 被某个实例抢占的时刻，用于崩溃回收
	SentAt      *time.Time // broker 确认的时刻

	// EventID 是这条消息对应的业务事件 ID，**在写入 outbox 时生成并持久化**。
	//
	// 为什么不能等到 PublishVideo 里再生成：publisher confirm 丢失时 poller 会重投，
	// 如果每次发布都新生成一个 event_id，同一条 outbox 消息前后两次投递带的 ID 不一样，
	// 消费端拿它做去重就完全失效 —— 而「confirm 丢失 ⇒ 必然重投」是 Outbox 无法消除的前提，
	// 所以幂等键必须从写入那一刻起就固定下来。
	//
	// 用指针是为了让历史行为 NULL（MySQL 唯一索引允许多个 NULL），
	// 否则 AutoMigrate 建索引时历史行会全落成 '' 并立刻违反唯一约束。
	EventID *string `gorm:"uniqueIndex:idx_outbox_event_id;size:64" json:"event_id,omitempty"`

	// ClaimToken 是每次抢占生成的 fencing 令牌。MarkSent/MarkRetry/MarkFailed 都要带它，
	// 否则「worker A 卡住 → 被回收 → worker B 抢占 → A 恢复后把 B 的消息标成 sent」
	// 会直接造成丢消息。见 outbox_repo.go 的 Claim。
	ClaimToken *string `gorm:"size:64" json:"-"`
}
