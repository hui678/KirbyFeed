package jwt

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/auth"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	// auth.jwtSecret 用 sync.Once 缓存，必须在本包任何测试签发 token 之前设置好。
	_ = os.Setenv("JWT_SECRET", strings.Repeat("k", 32))
	os.Exit(m.Run())
}

type fakeAccountStore struct {
	acc   *account.Account
	err   error
	calls int
}

func (f *fakeAccountStore) FindByID(ctx context.Context, id uint) (*account.Account, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.acc == nil || f.acc.ID != id {
		return nil, errors.New("account not found")
	}
	return f.acc, nil
}

func newTokenAndCache(t *testing.T) (*rediscache.Client, *miniredis.Miniredis, string, *auth.Claims) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cache := rediscache.NewClient(rdb, "v1:")

	token, err := auth.GenerateToken(7, "alice")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	claims, err := auth.ParseToken(token)
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	return cache, mr, token, claims
}

// 快路径命中即信任 —— 这个短路的正确性完全依赖一个不变量：
// 「Redis 里不会残留一个还能判活的旧值」。该不变量由 account.mustClear /
// setCurrentToken 保证（见 account/service_test.go 的三个回归用例）。
// 这里只锁定短路本身的行为，避免有人为了「更安全」改成每请求查库。
func TestIsActiveTrustsRedisHitWithoutTouchingDB(t *testing.T) {
	cache, mr, token, claims := newTokenAndCache(t)
	if err := mr.Set(cache.Key("account:%d", 7), token); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	store := &fakeAccountStore{err: errors.New("db must not be called")}

	if !isActive(context.Background(), claims, token, store, cache) {
		t.Fatal("expected active token to pass on redis hit")
	}
	if store.calls != 0 {
		t.Fatalf("expected no DB calls, got %d", store.calls)
	}
}

// P0 回归：吊销时 DEL 失败会落一张空哨兵（account.mustClear），
// 哨兵不等于任何真实 token，因此必然回源；MySQL 已清空 → 必须拒绝。
func TestIsActiveRejectsRevokedTokenWhenCacheHoldsSentinel(t *testing.T) {
	cache, mr, token, claims := newTokenAndCache(t)
	if err := mr.Set(cache.Key("account:%d", 7), ""); err != nil {
		t.Fatalf("seed sentinel: %v", err)
	}
	store := &fakeAccountStore{acc: &account.Account{ID: 7, Token: ""}}

	if isActive(context.Background(), claims, token, store, cache) {
		t.Fatal("revoked token must be rejected: sentinel must fall through to DB")
	}
	if store.calls != 1 {
		t.Fatalf("expected one DB fallback, got %d", store.calls)
	}
}

func TestIsActiveBackfillsCacheWithRemainingTokenTTL(t *testing.T) {
	cache, mr, token, claims := newTokenAndCache(t)
	store := &fakeAccountStore{acc: &account.Account{ID: 7, Token: token}}

	if !isActive(context.Background(), claims, token, store, cache) {
		t.Fatal("expected token from DB to pass")
	}
	key := cache.Key("account:%d", 7)
	if got, err := mr.Get(key); err != nil || got != token {
		t.Fatalf("expected cache backfill with token, got %q err=%v", got, err)
	}
	ttl := mr.TTL(key)
	if ttl <= 0 || ttl > auth.AccessTokenTTL {
		t.Fatalf("expected ttl in (0, %s], got %s", auth.AccessTokenTTL, ttl)
	}
}

// 空哨兵必须穿透回源：它不等于任何真实 token，但也不该让合法请求被误杀。
func TestIsActiveFallsThroughEmptySentinel(t *testing.T) {
	cache, mr, token, claims := newTokenAndCache(t)
	if err := mr.Set(cache.Key("account:%d", 7), ""); err != nil {
		t.Fatalf("seed sentinel: %v", err)
	}
	store := &fakeAccountStore{acc: &account.Account{ID: 7, Token: token}}

	if !isActive(context.Background(), claims, token, store, cache) {
		t.Fatal("empty sentinel must fall through to DB, where the token is still valid")
	}
	if store.calls != 1 {
		t.Fatalf("expected one DB fallback, got %d", store.calls)
	}
}
