package account

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	rediscache "feedsystem_video_go/internal/middleware/redis"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	// auth.jwtSecret 用 sync.Once 缓存，签发 access token 的测试必须先设置好。
	_ = os.Setenv("JWT_SECRET", strings.Repeat("k", 32))
	os.Exit(m.Run())
}

// failCmdHook 让指定命令失败，用来复现「Redis 还在、但这条命令写/删不成功」的部分故障。
type failCmdHook struct{ cmd string }

func (h failCmdHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h failCmdHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == h.cmd {
			return errors.New("injected " + h.cmd + " failure")
		}
		return next(ctx, cmd)
	}
}

func (h failCmdHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func newTestRedis(t *testing.T, hooks ...goredis.Hook) (*rediscache.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	for _, h := range hooks {
		rdb.AddHook(h)
	}
	return rediscache.NewClient(rdb, "v1:"), mr
}

func TestMustClearRemovesStaleValue(t *testing.T) {
	cache, mr := newTestRedis(t)
	svc := &AccountService{cache: cache}
	key := svc.accessTokenCacheKey(1)

	if err := mr.Set(key, "stale-token"); err != nil {
		t.Fatalf("seed stale value: %v", err)
	}
	svc.mustClear(context.Background(), key)

	if mr.Exists(key) {
		t.Fatal("expected stale token to be removed, key still exists")
	}
}

// 回归：新 token 写不进 Redis 时必须回退删除，否则被顶掉的旧 token 仍会被判活放行。
func TestSetCurrentTokenDropsStaleValueWhenWriteFails(t *testing.T) {
	cache, mr := newTestRedis(t, failCmdHook{cmd: "set"})
	svc := &AccountService{cache: cache}
	key := svc.accessTokenCacheKey(1)

	if err := mr.Set(key, "old-token"); err != nil {
		t.Fatalf("seed stale value: %v", err)
	}
	svc.setCurrentToken(context.Background(), 1, "new-token")

	if mr.Exists(key) {
		t.Fatal("expected stale token to be dropped after failed write, key still exists")
	}
}

// 回归：DEL 失败时必须落一张空哨兵，让判活必然穿透回源，而不是继续信任旧值。
func TestMustClearWritesEmptySentinelWhenDeleteFails(t *testing.T) {
	cache, mr := newTestRedis(t, failCmdHook{cmd: "del"})
	svc := &AccountService{cache: cache}
	key := svc.accessTokenCacheKey(1)

	if err := mr.Set(key, "stale-token"); err != nil {
		t.Fatalf("seed stale value: %v", err)
	}
	svc.mustClear(context.Background(), key)

	got, err := mr.Get(key)
	if err != nil {
		t.Fatalf("expected empty sentinel, get failed: %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty sentinel, got %q", got)
	}
}
