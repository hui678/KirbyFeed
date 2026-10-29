// Package videocache 保存视频实体缓存的 key、TTL 与原子脚本。
//
// 单独成包的原因：account 改名时需要失效作者名下所有视频的实体缓存，但
// account 不能 import video（video 的 handler 已经 import 了 account，会成环）。
// 把「key 怎么拼、失效和回写怎么做」下沉到一个只依赖 redis 中间件的叶子包，
// 既避开循环依赖，也避免同一个 key 格式在两处手写后各自漂移。
package videocache

import (
	"context"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

const (
	// EntityTTL 实体缓存过期时间。详情页与 Feed 批量读取共用同一份实体，
	// 两者必须共用 key 与 TTL，否则同一份数据会出现两份副本。
	EntityTTL = time.Hour

	// NullSentinel 是「这个 videoID 确实不存在」的占位值。
	//
	// 不能取 JSON 的 "null"：json.Unmarshal([]byte("null"), &v) 不报错，只会把 v
	// 留成零值，缓存出一个 ID=0 的垃圾实体。所以必须用非法 JSON 并显式识别。
	NullSentinel = "__null__"

	// NullTTL 故意取短：数据后来真补上了，最多延迟这么久就能读到。
	NullTTL = 60 * time.Second

	// VersionTTL 是失效版本号的存活时间，必须明显长于 EntityTTL：
	// 版本号是回写 CAS 的比较基准，比实体先过期的话，一次迟到的旧值回写
	// 就会重新通过校验，把旧数据再塞回去。
	VersionTTL = 25 * time.Hour
)

// opTimeout 是单次缓存操作的超时预算。每个操作独立申请，不复用同一个
// context —— 串联的 IO 会连续消耗同一份额度，最后一步必然超时。
const opTimeout = 50 * time.Millisecond

// EntityKey 返回视频实体缓存的 key。
func EntityKey(cache *rediscache.Client, id uint) string {
	return cache.Key("video:entity:%d", id)
}

// VersionKey 返回实体缓存的失效版本号 key。
//
// 每次失效都把这个数 +1；读路径在回源前记下它，回写时用 Lua 比较：
// 版本变了说明「读到的旧值已经过期」，直接放弃回写。这样堵住了
// 「DEL → 旧读回填」的经典竞态，而不需要延迟双删那种靠 sleep 赌时序的做法。
func VersionKey(cache *rediscache.Client, id uint) string {
	return cache.Key("video:entity:ver:%d", id)
}

// IsNull 判断取到的值是不是「确定不存在」占位值。
// 必须在 json.Unmarshal 之前调用：哨兵是非法 JSON，不先识别就会退化成普通 miss。
func IsNull(s string) bool {
	return s == NullSentinel
}

// SetNull 写入「确定不存在」占位值，nil 安全。
func SetNull(ctx context.Context, cache *rediscache.Client, id uint) {
	if cache == nil || id == 0 {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	_ = cache.SetBytes(opCtx, EntityKey(cache, id), []byte(NullSentinel), NullTTL)
}

// invalidateScript 把「版本 +1」与「删实体」合成一次原子操作。
// 分两条命令的话，回写可能插在 INCR 与 DEL 之间落地，然后又被 DEL 删掉 ——
// 结果不确定，取决于执行顺序。
const invalidateScript = `
local v = redis.call('INCR', KEYS[2])
redis.call('PEXPIRE', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[1])
return v
`

// Invalidate 失效实体缓存并递增版本号。
func Invalidate(ctx context.Context, cache *rediscache.Client, id uint) error {
	if cache == nil || id == 0 {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	_, err := cache.Eval(opCtx, invalidateScript,
		[]string{EntityKey(cache, id), VersionKey(cache, id)},
		VersionTTL.Milliseconds())
	return err
}

// setIfVersionScript 只有版本没变时才写入新值。
// ARGV[2] 为空串表示「回源前版本 key 不存在」，此后若发生过失效，cur 非空，校验失败。
const setIfVersionScript = `
local cur = redis.call('GET', KEYS[2])
if (cur or '') ~= ARGV[2] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
return 1
`

// SetIfVersion 在版本号仍等于 version 时写入实体，返回是否写入成功。
// 写入失败不一定是错误：版本变了说明期间发生过失效，本次回写本就该丢弃。
func SetIfVersion(ctx context.Context, cache *rediscache.Client, id uint, payload []byte, ttl time.Duration, version string) (bool, error) {
	if cache == nil || id == 0 {
		return false, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	n, err := cache.Eval(opCtx, setIfVersionScript,
		[]string{EntityKey(cache, id), VersionKey(cache, id)},
		payload, version, ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
