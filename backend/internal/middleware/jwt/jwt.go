package jwt

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/auth"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

// AccountTokenStore 收窄判活对账号存储的依赖：判活只需要「按 ID 读一行」。
// 用接口而不是具体 Repository，才能在不接 MySQL 的情况下覆盖吊销路径。
type AccountTokenStore interface {
	FindByID(ctx context.Context, id uint) (*account.Account, error)
}

// bearerToken 解析 Authorization 头。
// 返回值：(token 字符串, 头部是否格式正确)。空串 + true 表示「压根没带」。
func bearerToken(c *gin.Context) (string, bool) {
	authHeader := c.GetHeader("Authorization")
	if strings.TrimSpace(authHeader) == "" {
		return "", true
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", true
	}
	return token, true
}

// JWTAuth 硬鉴权：没有有效 token 直接 401，用于写接口和必须登录才能看的接口。
func JWTAuth(accountRepo AccountTokenStore, cache *rediscache.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString, ok := bearerToken(c)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization header"})
			return
		}
		if tokenString == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			return
		}

		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}
		if !isActive(c.Request.Context(), claims, tokenString, accountRepo, cache) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
			return
		}

		c.Set("accountID", claims.AccountID)
		c.Set("username", claims.Username)
		c.Next()
	}
}

// SoftJWTAuth 软鉴权：能不能匿名是接口自己说了算，中间件只负责「有就认，没有/坏了就当匿名」。
//
// 修复要点：旧实现在 token 解析失败（过期或签名非法）时直接 401，
// 结果用户揣着一张过期 token 刷公开 Feed 会整页报错 —— 这跟「匿名可浏览」的设计目标相悖。
// 现在的语义是：任何一步拿不到可信身份，就退化成匿名继续，绝不阻断公开内容。
func SoftJWTAuth(accountRepo AccountTokenStore, cache *rediscache.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString, ok := bearerToken(c)
		if !ok || tokenString == "" {
			c.Next()
			return
		}

		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			c.Next()
			return
		}
		// 已吊销（登出/改密/互踢）同样按匿名处理，而不是报 401。
		if !isActive(c.Request.Context(), claims, tokenString, accountRepo, cache) {
			c.Next()
			return
		}

		c.Set("accountID", claims.AccountID)
		c.Set("username", claims.Username)
		c.Next()
	}
}

// VerifyToken 供非 HTTP 中间件场景（如 SSE 握手）复用同一套校验规则。
func VerifyToken(ctx context.Context, tokenString string, accountRepo AccountTokenStore, cache *rediscache.Client) (*auth.Claims, bool) {
	claims, err := auth.ParseToken(tokenString)
	if err != nil {
		return nil, false
	}
	if !isActive(ctx, claims, tokenString, accountRepo, cache) {
		return nil, false
	}
	return claims, true
}

// isActive 判断「这张 access token 是不是当前生效的那一张」。
//
// 判活 = 比对服务端记录的当前 token，而不是只看 JWT 签名：
// 签名的 JWT 无法在过期前作废，要支持登出/改密即时吊销就必须引入服务端状态。
//
// 校验顺序：Redis 快路径 -> 不一致或未命中则回源 MySQL。
// 注意「Redis 里的值 != 传入 token」时也回源 DB，而不是直接判死：
// DB 才是权威，Redis 只是快路径；若登录时 Redis 写入失败而 DB 写入成功，
// 直接判死会让刚登录的用户拿着合法 token 被 401。登出时两侧都删，吊销依然即时。
func isActive(ctx context.Context, claims *auth.Claims, tokenString string, accountRepo AccountTokenStore, cache *rediscache.Client) bool {
	if claims == nil || claims.AccountID == 0 {
		return false
	}
	key := cache.Key("account:%d", claims.AccountID)

	// 快路径：Redis
	if cache != nil {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		b, err := cache.GetBytes(cacheCtx, key)
		cancel()
		// b 为空串是吊销时写的哨兵（见 account.setCurrentToken/mustClear），
		// 它不可能等于任何真实 token，因此必然落到下面的回源分支。
		if err == nil && len(b) > 0 && string(b) == tokenString {
			return true
		}
	}

	// 回源：MySQL
	accountInfo, err := accountRepo.FindByID(ctx, claims.AccountID)
	if err != nil || accountInfo == nil || accountInfo.Token == "" || accountInfo.Token != tokenString {
		return false
	}

	// 回填快路径，避免后续每个请求都回源。
	//
	// TTL 必须取「token 剩余寿命」而不是固定 24h：这里回填的值是从 MySQL 读出来的权威值，
	// 理论上与 DB 一致，但我们无法保证 service 侧每一次更新/删除都成功写到这里。
	// 把 TTL 压到不超过 exp，等于给可能的残留脏值设了一个硬上限 ——
	// 最坏情况下多放行的时间不超过一个 access token 的生命周期（15 分钟），而非原来的 24 小时。
	if cache != nil {
		ttl := tokenRemainingTTL(claims)
		if ttl > 0 {
			cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			if err := cache.SetBytes(cacheCtx, key, []byte(tokenString), ttl); err != nil {
				log.Printf("failed to set cache: %v", err)
			}
		}
	}

	return true
}

// tokenRemainingTTL 计算 access token 还剩多久过期。
// 取不到过期时间时返回 0（拒绝回填），宁可让后续请求回源 MySQL。
func tokenRemainingTTL(claims *auth.Claims) time.Duration {
	if claims == nil || claims.ExpiresAt == nil {
		return 0
	}
	ttl := time.Until(claims.ExpiresAt.Time)
	if ttl <= 0 {
		return 0
	}
	if ttl > 24*time.Hour {
		return 24 * time.Hour
	}
	return ttl
}

func GetAccountID(c *gin.Context) (uint, error) {
	uidValue, exists := c.Get("accountID")
	if !exists {
		return 0, errors.New("accountID not found")
	}

	accountID, ok := uidValue.(uint)
	if !ok {
		return 0, errors.New("accountID has invalid type")
	}

	return accountID, nil
}

func GetUsername(c *gin.Context) (string, error) {
	val, exists := c.Get("username")
	if !exists {
		return "", errors.New("username not found")
	}

	username, ok := val.(string)
	if !ok {
		return "", errors.New("username has invalid type")
	}

	return username, nil
}
