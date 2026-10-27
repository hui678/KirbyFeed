package video

import (
	"context"
	"fmt"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"

	"golang.org/x/sync/singleflight"
)

// singleRedisOpTimeout 是单个 Redis 操作的超时预算。
//
// 带 timeout 的 context 的额度会被串联在其上的所有 IO 操作连续消耗，
// 因此每个 Redis 操作都必须各自新建 context，不能跨步骤复用。
const singleRedisOpTimeout = 50 * time.Millisecond

// VideoEntityTTL 视频实体缓存过期时间。
// 详情页读取与 Feed 批量读取共用同一份实体缓存（video:entity:<id>），
// 两者必须共用同一个 key 构造方式与同一个 TTL，否则同一份数据会出现两份副本。
const VideoEntityTTL = time.Hour

// VideoEntityKey 返回视频实体缓存的 key。
func VideoEntityKey(cache *rediscache.Client, id uint) string {
	return cache.Key("video:entity:%d", id)
}

// VideoEntityNullSentinel 是「这个 videoID 确实不存在」的占位值。
//
// 为什么需要：L3 查不到时如果什么都不记，同一个不存在的 ID 每次请求都会打
// 一次 MySQL —— 这就是**缓存穿透**。写入这个哨兵后，后续请求在 L2 就能判定
// 「确定不存在」，不再回源。
//
// 为什么不是 JSON 的 "null"：json.Unmarshal([]byte("null"), &v) 不报错，
// 只会把 v 留成零值 —— 那会缓存出一个 ID=0 的垃圾实体。所以哨兵必须取一个
// 非法 JSON，并且必须在尝试解析之前显式识别（见 IsVideoEntityNull）。
const VideoEntityNullSentinel = "__null__"

// VideoEntityNullTTL 占位值的 TTL，故意取短：万一数据后来真的补上了，最多
// 延迟这么久就能读到。这也是它相对布隆过滤器的优势 —— 后者不支持删除，
// 已失效的 ID 会永久残留、让误判率持续上升。
const VideoEntityNullTTL = 60 * time.Second

// IsVideoEntityNull 判断缓存里取到的值是不是「确定不存在」的占位值。
// 必须在 json.Unmarshal 之前调用：哨兵是非法 JSON，不先识别就会退化成普通 miss。
func IsVideoEntityNull(s string) bool {
	return s == VideoEntityNullSentinel
}

// SetVideoEntityNull 写入「这个 videoID 确定不存在」的占位值，nil 安全。
func SetVideoEntityNull(ctx context.Context, cache *rediscache.Client, id uint) {
	if cache == nil || id == 0 {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
	defer cancel()
	_ = cache.SetBytes(opCtx, VideoEntityKey(cache, id), []byte(VideoEntityNullSentinel), VideoEntityNullTTL)
}

// InvalidateVideoEntity 失效视频实体缓存，nil 安全的空实现。
func InvalidateVideoEntity(ctx context.Context, cache *rediscache.Client, id uint) {
	if cache == nil || id == 0 {
		return
	}
	_ = cache.Del(ctx, VideoEntityKey(cache, id))
}

// EntityLoadGroup 视频实体回源的进程内去重组。
//
// 详情页读取（VideoService.GetDetail）与 Feed 批量读取（FeedService.GetVideoByIDs）
// 命中的是同一个 video:entity:<id>，回源走的也是同一个主键单行查询。
// 两者必须共用同一个 Group 且用同一个 key，并发回源才能真正合并；
// 若各自持有一个 Group，同一视频在同一时刻仍会被每个 Group 各放行一次，
// 「一份数据两套机制」的并联问题并不会因为都改成 singleflight 而消失。
//
// 选 singleflight 而不是 Redis 互斥锁的理由：
//  1. 主键单行查询回源成本极小，但并发量很大 —— 分布式锁「每个请求多付 2 次
//     Redis 往返」的代价，远超「省下 (P-1) 次主键查询」的收益；
//  2. 等待方直接拿到 leader 的返回值，不依赖 leader 是否成功回填缓存；
//  3. 不依赖 Redis，缓存层不可用时这条防线依然生效。
var EntityLoadGroup singleflight.Group

// EntitySingleflightKey 返回视频实体回源去重用的 key。
// 这是进程内存里的 key，不写入 Redis，因此不需要 cache.Key 的版本前缀。
func EntitySingleflightKey(id uint) string {
	return fmt.Sprintf("sf:entity:%d", id)
}
