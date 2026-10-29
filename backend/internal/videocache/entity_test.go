package videocache

import (
	"context"
	"fmt"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

func newTestCache(t *testing.T) (*rediscache.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return rediscache.NewClient(goredis.NewClient(&goredis.Options{Addr: mr.Addr()}), "v1:"), mr
}

func TestSetIfVersionDiscardsWriteAfterInvalidate(t *testing.T) {
	ctx := context.Background()
	cache, _ := newTestCache(t)

	// 没有任何失效记录时版本为空串，首次回写应当成功。
	ok, err := SetIfVersion(ctx, cache, 42, []byte(`{"id":42}`), time.Minute, "")
	if err != nil || !ok {
		t.Fatalf("initial write: ok=%v err=%v", ok, err)
	}

	// 失效后旧版本的回写必须被拒：这正是「回源期间数据被改」的竞态。
	if err := Invalidate(ctx, cache, 42); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	ok, err = SetIfVersion(ctx, cache, 42, []byte(`{"id":42,"stale":true}`), time.Minute, "")
	if err != nil {
		t.Fatalf("stale write: %v", err)
	}
	if ok {
		t.Fatal("stale write should be rejected after invalidate")
	}
	exists, err := cache.Exists(ctx, EntityKey(cache, 42))
	if err != nil {
		t.Fatalf("check entity: %v", err)
	}
	if exists {
		t.Fatal("entity should remain absent after rejected stale write")
	}

	// 读到新版本后回写才被接受。
	b, err := cache.GetBytes(ctx, VersionKey(cache, 42))
	if err != nil {
		t.Fatalf("read version: %v", err)
	}
	ok, err = SetIfVersion(ctx, cache, 42, []byte(`{"id":42,"fresh":true}`), time.Minute, string(b))
	if err != nil || !ok {
		t.Fatalf("fresh write: ok=%v err=%v", ok, err)
	}
}

func TestInvalidateBumpsVersion(t *testing.T) {
	ctx := context.Background()
	cache, _ := newTestCache(t)

	for i := 1; i <= 2; i++ {
		if err := Invalidate(ctx, cache, 7); err != nil {
			t.Fatalf("invalidate %d: %v", i, err)
		}
		b, err := cache.GetBytes(ctx, VersionKey(cache, 7))
		if err != nil {
			t.Fatalf("read version: %v", err)
		}
		want := fmt.Sprintf("%d", i)
		if got := string(b); got != want {
			t.Fatalf("version after %d invalidations = %q, want %q", i, got, want)
		}
	}
}
