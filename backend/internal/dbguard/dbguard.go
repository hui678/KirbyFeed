// Package dbguard 是进程级的数据库回源闸门。
//
// 缓存层整体失效时，唯一还挡在 MySQL 前面的应用层防线就是它：拿不到配额
// 立刻失败（503），而不是排队 —— 排队只是把堆积从数据库搬到应用内存，
// 连接池仍然会被慢查询占满，压力最终还是会落到库上。
//
// 容量由连接池上限推导（取一半），在 db.Open 里配置一次：连接池还要留给
// 写路径、事务、迁移和其他包的查询，回源闸门不能吃满。
package dbguard

import (
	"fmt"

	"feedsystem_video_go/internal/apierror"
)

// ErrOverloaded 表示本期回源并发已达上限，请求被主动丢弃以保护数据库。
// 它包装 apierror.ErrServiceOverloaded，HTTP 层会分类成 503 而不是 500：
// 这是主动降级，客户端应当退避重试。
var ErrOverloaded = fmt.Errorf("db load gate full, shedding load: %w", apierror.ErrServiceOverloaded)

// slots 为 nil 表示尚未配置，此时 Acquire 一律放行（不误伤只跑单测/工具的进程）。
var slots chan struct{}

// Configure 按连接池上限设置闸门容量。必须在启动阶段、并发请求开始前调用。
func Configure(maxOpenConns int) {
	if maxOpenConns <= 0 {
		maxOpenConns = 1
	}
	capacity := maxOpenConns / 2
	if capacity < 1 {
		capacity = 1
	}
	slots = make(chan struct{}, capacity)
}

// Capacity 返回当前闸门容量，0 表示未配置（不限流）。
func Capacity() int {
	return cap(slots)
}

// Acquire 尝试占用一个回源配额。非阻塞：拿不到立即返回 false。
func Acquire() bool {
	if slots == nil {
		return true
	}
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release 归还配额。
func Release() {
	if slots == nil {
		return
	}
	<-slots
}

// Do 占用配额执行一次数据库操作；配额已满时直接返回 ErrOverloaded。
func Do(fn func() error) error {
	if !Acquire() {
		return ErrOverloaded
	}
	defer Release()
	return fn()
}
