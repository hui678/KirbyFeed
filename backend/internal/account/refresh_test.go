package account

import (
	"context"
	"errors"
	"testing"
	"time"

	"feedsystem_video_go/internal/auth"

	miniredis "github.com/alicebob/miniredis/v2"
)

// fakeAccountRepo 用内存实现替代 MySQL，专门用来验证刷新链路的判定顺序。
// 它模拟真实 repo 的关键语义：refresh 同时匹配 current 与 prev、轮换是 CAS。
type fakeAccountRepo struct {
	account           *Account
	rotateOK          bool
	rotateErr         error
	rotateCall        int
	logoutCall        int
	afterFailedRotate func()
}

func (f *fakeAccountRepo) CreateAccount(ctx context.Context, account *Account) error { return nil }

func (f *fakeAccountRepo) FindByID(ctx context.Context, id uint) (*Account, error) {
	if f.account == nil || f.account.ID != id {
		return nil, errors.New("account not found")
	}
	return f.account, nil
}

func (f *fakeAccountRepo) FindByUsername(ctx context.Context, username string) (*Account, error) {
	if f.account == nil || f.account.Username != username {
		return nil, errors.New("account not found")
	}
	return f.account, nil
}

func (f *fakeAccountRepo) Login(ctx context.Context, id uint, token, refreshTokenHash string, refreshExpiry time.Time) error {
	f.account.Token = token
	f.account.RefreshToken = refreshTokenHash
	f.account.PrevRefreshToken = ""
	f.account.RefreshRotatedAt = time.Time{}
	f.account.RefreshTokenExpiry = refreshExpiry
	return nil
}

func (f *fakeAccountRepo) Logout(ctx context.Context, id uint) error {
	f.logoutCall++
	f.account.Token = ""
	f.account.RefreshToken = ""
	f.account.PrevRefreshToken = ""
	f.account.RefreshRotatedAt = time.Time{}
	return nil
}

func (f *fakeAccountRepo) ChangePassword(ctx context.Context, id uint, newPassword string) error {
	return nil
}

func (f *fakeAccountRepo) RenameWithToken(ctx context.Context, id uint, newUsername string, token string) ([]uint, error) {
	return nil, nil
}

func (f *fakeAccountRepo) UpdateAvatar(ctx context.Context, accountID uint, avatarURL string) error {
	return nil
}

func (f *fakeAccountRepo) UpdateFields(ctx context.Context, id uint, updates map[string]interface{}) error {
	return nil
}

func (f *fakeAccountRepo) UpdateToken(ctx context.Context, id uint, token string) error {
	f.account.Token = token
	return nil
}

func (f *fakeAccountRepo) FindByRefreshToken(ctx context.Context, refreshTokenHash string) (*Account, error) {
	if f.account == nil {
		return nil, errors.New("account not found")
	}
	if f.account.RefreshToken != refreshTokenHash && f.account.PrevRefreshToken != refreshTokenHash {
		return nil, errors.New("account not found")
	}
	return f.account, nil
}

func (f *fakeAccountRepo) RotateRefresh(ctx context.Context, id uint, oldHash, newHash string, rotatedAt time.Time) (bool, error) {
	f.rotateCall++
	if f.rotateErr != nil {
		return false, f.rotateErr
	}
	if !f.rotateOK {
		if f.afterFailedRotate != nil {
			f.afterFailedRotate()
		}
		return false, nil
	}
	if f.account.RefreshToken != oldHash {
		return false, nil
	}
	f.account.PrevRefreshToken = oldHash
	f.account.RefreshToken = newHash
	f.account.RefreshRotatedAt = rotatedAt
	return true, nil
}

func newRefreshFixture(t *testing.T) (*AccountService, *fakeAccountRepo, *miniredis.Miniredis, *Account, string) {
	t.Helper()
	cache, mr := newTestRedis(t)
	raw := "refresh-token-1"
	repo := &fakeAccountRepo{
		rotateOK: true,
		account: &Account{
			ID:                 7,
			Username:           "alice",
			Token:              "old-access",
			RefreshToken:       auth.HashRefreshToken(raw),
			RefreshTokenExpiry: time.Now().Add(auth.RefreshTokenTTL),
		},
	}
	svc := &AccountService{accountRepository: repo, cache: cache}
	return svc, repo, mr, repo.account, raw
}

func TestRefreshRotatesTokenAndInvalidatesOld(t *testing.T) {
	svc, _, mr, acc, oldRaw := newRefreshFixture(t)

	result, err := svc.RefreshAccessToken(context.Background(), oldRaw)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if result.RefreshToken == oldRaw {
		t.Fatal("expected refresh token to be rotated")
	}
	if acc.RefreshToken != auth.HashRefreshToken(result.RefreshToken) {
		t.Fatal("expected DB to store hash of the new refresh token")
	}
	if acc.PrevRefreshToken != auth.HashRefreshToken(oldRaw) {
		t.Fatal("expected previous refresh hash to be recorded for reuse detection")
	}
	if _, err := auth.ParseToken(result.AccessToken); err != nil {
		t.Fatalf("expected a valid access token, got %v", err)
	}
	// 旧反查键必须删除，新反查键必须存在
	if mr.Exists(svc.cache.Key("refresh:%s", auth.HashRefreshToken(oldRaw))) {
		t.Fatal("old refresh lookup key must be deleted")
	}
	if got, err := mr.Get(svc.cache.Key("refresh:%s", acc.RefreshToken)); err != nil || got != "7" {
		t.Fatalf("expected new refresh lookup key, got %q err=%v", got, err)
	}
}

func TestRefreshReplayWithinGraceReturnsSameTokens(t *testing.T) {
	svc, repo, _, acc, oldRaw := newRefreshFixture(t)

	first, err := svc.RefreshAccessToken(context.Background(), oldRaw)
	if err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	// 模拟「客户端没收到响应，用旧票重试」。
	acc.RefreshRotatedAt = time.Now()
	second, err := svc.RefreshAccessToken(context.Background(), oldRaw)
	if err != nil {
		t.Fatalf("replay within grace must not fail: %v", err)
	}
	if second.RefreshToken != first.RefreshToken {
		t.Fatal("replay must return the same rotated refresh token")
	}
	if repo.rotateCall != 1 {
		t.Fatalf("replay must not rotate again, rotate calls = %d", repo.rotateCall)
	}
}

func TestRefreshReuseOutsideGraceRevokesAccount(t *testing.T) {
	svc, repo, mr, acc, oldRaw := newRefreshFixture(t)

	if _, err := svc.RefreshAccessToken(context.Background(), oldRaw); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	// 旧票在宽限期外再次出现 —— 判定为泄漏。
	acc.RefreshRotatedAt = time.Now().Add(-2 * refreshReuseGrace)
	_, err := svc.RefreshAccessToken(context.Background(), oldRaw)
	if !errors.Is(err, ErrRefreshReuse) {
		t.Fatalf("expected ErrRefreshReuse, got %v", err)
	}
	if repo.logoutCall != 1 {
		t.Fatalf("expected account tokens to be revoked, logout calls = %d", repo.logoutCall)
	}
	if acc.Token != "" || acc.RefreshToken != "" {
		t.Fatal("expected current access/refresh to be cleared")
	}
	if mr.Exists(svc.cache.Key("account:%d", acc.ID)) {
		t.Fatal("expected access fast-path to be cleared on reuse")
	}
}

func TestRefreshConcurrentRotationFallsBackToReplay(t *testing.T) {
	svc, repo, _, acc, oldRaw := newRefreshFixture(t)

	newRaw := "refresh-token-2"
	// CAS 失败：读到的还是 oldRaw，但改的时候发现它已经被别人换掉了。
	repo.rotateOK = false
	repo.afterFailedRotate = func() {
		acc.PrevRefreshToken = auth.HashRefreshToken(oldRaw)
		acc.RefreshToken = auth.HashRefreshToken(newRaw)
		acc.RefreshRotatedAt = time.Now()
	}
	svc.stashIssuedRefresh(context.Background(), auth.HashRefreshToken(oldRaw), newRaw)

	result, err := svc.RefreshAccessToken(context.Background(), oldRaw)
	if err != nil {
		t.Fatalf("concurrent refresh should replay: %v", err)
	}
	if result.RefreshToken != newRaw {
		t.Fatalf("expected the already-rotated refresh token, got %q", result.RefreshToken)
	}
	if repo.rotateCall != 1 {
		t.Fatalf("expected one CAS attempt, got %d", repo.rotateCall)
	}
}

func TestRefreshRejectsExpiredToken(t *testing.T) {
	svc, _, _, acc, raw := newRefreshFixture(t)
	acc.RefreshTokenExpiry = time.Now().Add(-time.Second)

	if _, err := svc.RefreshAccessToken(context.Background(), raw); !errors.Is(err, ErrRefreshExpired) {
		t.Fatalf("expected ErrRefreshExpired, got %v", err)
	}
}
