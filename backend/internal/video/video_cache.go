package video

import (
	"context"
	"fmt"
	"log"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/videocache"

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
const VideoEntityTTL = videocache.EntityTTL

// VideoEntityKey 返回视频实体缓存的 key。
func VideoEntityKey(cache *rediscache.Client, id uint) string {
	return videocache.EntityKey(cache, id)
}

// VideoEntityVersionKey 返回实体缓存的失效版本号 key。
// Feed 的批量读取会把这些 key 一次 MGET 出来，作为回写 CAS 的比较基准。
func VideoEntityVersionKey(cache *rediscache.Client, id uint) string {
	return videocache.VersionKey(cache, id)
}

// VideoEntityNullSentinel 是「这个 videoID 确实不存在」的占位值，详见 videocache 包。
const VideoEntityNullSentinel = videocache.NullSentinel

// VideoEntityNullTTL 占位值的 TTL，故意取短：万一数据后来真的补上了，最多
// 延迟这么久就能读到。这也是它相对布隆过滤器的优势 —— 后者不支持删除，
// 已失效的 ID 会永久残留、让误判率持续上升。
const VideoEntityNullTTL = videocache.NullTTL

// IsVideoEntityNull 判断缓存里取到的值是不是「确定不存在」的占位值。
// 必须在 json.Unmarshal 之前调用：哨兵是非法 JSON，不先识别就会退化成普通 miss。
func IsVideoEntityNull(s string) bool {
	return videocache.IsNull(s)
}

// SetVideoEntityNull 写入「这个 videoID 确定不存在」的占位值，nil 安全。
func SetVideoEntityNull(ctx context.Context, cache *rediscache.Client, id uint) {
	videocache.SetNull(ctx, cache, id)
}

// InvalidateVideoEntity 失效视频实体缓存：DEL 实体 + 版本号 +1（原子）。
// 版本号递增是 Q14 那道「旧值迟到回填」的解法，详见 videocache.Invalidate。
func InvalidateVideoEntity(ctx context.Context, cache *rediscache.Client, id uint) {
	if err := videocache.Invalidate(ctx, cache, id); err != nil {
		log.Printf("invalidate video entity failed (video=%d): %v", id, err)
	}
}

// EntityVersion 读取实体当前的失效版本号。
// 第二个返回值 false 表示 Redis 不可用或读失败 —— 此时不应回写缓存，
// 因为 CAS 的比较基准不可信。
func EntityVersion(ctx context.Context, cache *rediscache.Client, id uint) (string, bool) {
	if cache == nil {
		return "", false
	}
	opCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
	defer cancel()

	b, err := cache.GetBytes(opCtx, videocache.VersionKey(cache, id))
	if err != nil {
		if rediscache.IsMiss(err) {
			return "", true // 从未失效过：版本号视为空串
		}
		return "", false
	}
	return string(b), true
}

// SetVideoEntityIfVersion 只有版本号未变时才写入实体。
// 返回 false 表示期间发生过失效（或写入失败），本次回写被丢弃。
func SetVideoEntityIfVersion(ctx context.Context, cache *rediscache.Client, id uint, payload []byte, ttl time.Duration, version string) bool {
	ok, err := videocache.SetIfVersion(ctx, cache, id, payload, ttl, version)
	if err != nil {
		log.Printf("write back video entity failed (video=%d): %v", id, err)
		return false
	}
	return ok
}

// maxPendingEntityWrites 限制进程内同时在途的异步缓存回写数。
//
// 每个 L3 miss 都起一个 goroutine 回写，在 Redis 故障时会线性堆积
// （每个 goroutine 都要等到自己的超时）。有界之后超出的回写直接丢弃：
// 回写只是加速下一次读，丢了不影响正确性，下一次 miss 会再补。
const maxPendingEntityWrites = 256

var entityWriteSlots = make(chan struct{}, maxPendingEntityWrites)

// ScheduleEntityWrite 在配额内异步执行一次缓存回写，配额满时丢弃并返回 false。
func ScheduleEntityWrite(fn func()) bool {
	select {
	case entityWriteSlots <- struct{}{}:
		go func() {
			defer func() { <-entityWriteSlots }()
			fn()
		}()
		return true
	default:
		return false
	}
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
