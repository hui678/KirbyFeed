// internal/auth/jwt.go
package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// AccessTokenTTL：Access Token 的生命周期。短 TTL 的意义是「泄漏后损失可控」，
	// 代价是必须靠 Refresh Token 续期，鉴权链路每次都要能快速判活。
	AccessTokenTTL = 15 * time.Minute
	// RefreshTokenTTL：Refresh Token 的生命周期，决定「用户多久必须重新登录一次」。
	// 它同时是 Redis 侧 refresh 相关 key 的 TTL，也是 MySQL 侧 refresh_token_expiry 的取值，两边必须同源。
	RefreshTokenTTL = 7 * 24 * time.Hour
)

// minSecretLen：HS256 要求密钥长度不低于 32 字节才有意义，低于该值只警告不拒绝，
// 避免把本地/演示环境直接挡在门外，但生产环境应当在启动日志里看到这条警告并整改。
const minSecretLen = 32

var (
	secretOnce sync.Once
	secret     []byte
	secretErr  error
)

// Init 在进程启动时显式校验 JWT 密钥。
//
// 修复要点：旧实现在 JWT_SECRET 缺失时会「随机生成一把密钥」，等价于
//   - 进程重启后所有已签发 token 全部失效；
//   - 多实例之间密钥不同，A 实例签发的 token 到 B 实例校验失败（表现为随机 401）。
//
// 鉴权密钥必须是部署时注入的确定值，因此这里改成 fail-fast：缺失直接启动失败。
func Init() error {
	_, err := jwtSecret()
	return err
}

// jwtSecret 只解析一次并缓存，sync.Once 保证并发安全。
// 旧实现直接读写全局变量 cachedSecret，多 goroutine 同时首次调用会数据竞争（-race 必报）。
func jwtSecret() ([]byte, error) {
	secretOnce.Do(func() {
		s := strings.TrimSpace(os.Getenv("JWT_SECRET"))
		if s == "" {
			secretErr = errors.New("JWT_SECRET is not set")
			return
		}
		if len(s) < minSecretLen {
			log.Printf("WARNING: JWT_SECRET is only %d bytes, recommend >= %d bytes", len(s), minSecretLen)
		}
		secret = []byte(s)
	})
	return secret, secretErr
}

type Claims struct {
	AccountID uint   `json:"account_id"`
	Username  string `json:"username"`
	jwt.RegisteredClaims
}

func GenerateToken(accountID uint, username string) (string, error) {
	secret, err := jwtSecret()
	if err != nil {
		return "", err
	}
	now := time.Now()

	claims := Claims{
		AccountID: accountID,
		Username:  username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(AccessTokenTTL)), //过期时间
			IssuedAt:  jwt.NewNumericDate(now),                     //签发时间
			NotBefore: jwt.NewNumericDate(now),                     //生效时间
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	return token.SignedString(secret)
}

// GenerateRefreshToken 生成不透明（opaque）随机串，而不是 JWT。
// 原因：refresh 凭证需要在服务端「可吊销」，JWT 自带无状态特性反而是负担；
// 随机串只能靠 Redis/MySQL 比对，天然支持登出即失效。
func GenerateRefreshToken(accountID uint) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func ParseToken(tokenString string) (*Claims, error) {
	secret, err := jwtSecret()
	if err != nil {
		return nil, err
	}
	token, err := jwt.ParseWithClaims(
		tokenString,
		&Claims{},
		func(token *jwt.Token) (interface{}, error) {
			if token.Method == nil || token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, errors.New("unexpected signing method")
			}
			return secret, nil
		},
	)
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, jwt.ErrTokenInvalidClaims
	}

	return claims, nil
}
