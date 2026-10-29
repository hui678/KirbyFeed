package feed

import "time"

type FeedAuthor struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

type FeedVideoItem struct {
	ID          uint       `json:"id"`
	Author      FeedAuthor `json:"author"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	PlayURL     string     `json:"play_url"`
	CoverURL    string     `json:"cover_url"`
	CreateTime  int64      `json:"create_time"`
	LikesCount  int64      `json:"likes_count"`
	IsLiked     bool       `json:"is_liked"`
}

// TimelineCursor 时间线的**复合**游标 (create_time, id)。
//
// 为什么不能只用 create_time：同一毫秒可能发布多条视频，纯时间游标会把它们
// 当成一个不可分的整体 —— 要么全部跳过（漏项），要么因 MySQL 在相同 create_time
// 之间没有稳定顺序而被重复返回。加上 id 作为次级键后，"游标之后"才是严格全序的。
//
// 排序规则统一为 (create_time DESC, id DESC)，Redis 侧与 MySQL 侧必须完全一致。
type TimelineCursor struct {
	CreateTime time.Time
	ID         uint
}

type ListLatestRequest struct {
	Limit          int   `json:"limit"`
	LatestTime     int64 `json:"latest_time"`
	LatestIDBefore *uint `json:"latest_id_before,omitempty"`
}

type ListLatestResponse struct {
	VideoList    []FeedVideoItem `json:"video_list"`
	NextTime     int64           `json:"next_time"`
	NextIDBefore *uint           `json:"next_id_before,omitempty"`
	HasMore      bool            `json:"has_more"`
}

type ListByFollowingRequest struct {
	Limit          int   `json:"limit"`
	LatestTime     int64 `json:"latest_time"`
	LatestIDBefore *uint `json:"latest_id_before,omitempty"`
}

type ListByFollowingResponse struct {
	VideoList    []FeedVideoItem `json:"video_list"`
	NextTime     int64           `json:"next_time"`
	NextIDBefore *uint           `json:"next_id_before,omitempty"`
	HasMore      bool            `json:"has_more"`
}

type ListByPopularityRequest struct {
	Limit          int   `json:"limit"`
	AsOf           int64 `json:"as_of"`  // 服务器返回的分钟时间戳；第一页传0
	Offset         int   `json:"offset"` // 下一页从这里开始；第一页传0
	LatestIDBefore *uint `json:"latest_id_before,omitempty"`

	// ForceDB 让「已经降级的分页会话」继续留在 MySQL 累计热度排序上。
	// Redis 中途恢复时不能悄悄切回窗口榜，否则同一会话会拼两套排序。
	ForceDB bool `json:"force_db,omitempty"`

	// DB fallback 用（可选）
	LatestPopularity int64     `json:"latest_popularity"`
	LatestBefore     time.Time `json:"latest_before"`
}

type ListByPopularityResponse struct {
	VideoList  []FeedVideoItem `json:"video_list"`
	AsOf       int64           `json:"as_of"`
	NextOffset int             `json:"next_offset"`
	HasMore    bool            `json:"has_more"`

	// Degraded 表示本次结果来自 MySQL 累计热度榜（Redis 不可用），
	// 与窗口热榜是两套排序语义，客户端应提示并保持同一套排序翻页。
	Degraded bool `json:"degraded,omitempty"`
	// Expired 表示客户端带来的 as_of 已滑出可翻页窗口，需要从第一页重新开始。
	Expired bool `json:"expired,omitempty"`

	NextLatestPopularity *int64     `json:"next_latest_popularity,omitempty"`
	NextLatestBefore     *time.Time `json:"next_latest_before,omitempty"`
	NextLatestIDBefore   *uint      `json:"next_latest_id_before,omitempty"`
}

type TagSummary struct {
	Name       string `json:"name"`
	VideoCount int64  `json:"video_count"`
}

type ListTagsRequest struct {
	Limit int `json:"limit"`
}

type ListTagsResponse struct {
	Tags []TagSummary `json:"tags"`
}

type ListByTagRequest struct {
	TagName        string `json:"tag_name"`
	Limit          int    `json:"limit"`
	LatestTime     int64  `json:"latest_time"`
	LatestIDBefore *uint  `json:"latest_id_before,omitempty"`
}

type ListByTagResponse struct {
	VideoList    []FeedVideoItem `json:"video_list"`
	NextTime     int64           `json:"next_time"`
	NextIDBefore *uint           `json:"next_id_before,omitempty"`
	HasMore      bool            `json:"has_more"`
}
