package video

import rediscache "feedsystem_video_go/internal/middleware/redis"

// TimelineCapacity 是热时间线保留的视频条数上限。
// 写入裁剪、空 ZSET 重建、后台对账三处必须引用同一个值：
// 任何一处用了不同的数字，都会出现「刚重建就被裁掉」或「对账补完又超量」的抖动。
const TimelineCapacity = 1000

const timelineKeyName = "feed:global_timeline"

// TimelineZSetKey 返回热时间线的 Redis key（带命名空间前缀）。
func TimelineZSetKey(cache *rediscache.Client) string {
	return cache.Key(timelineKeyName)
}
