package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"feedsystem_video_go/internal/account"
	jwt "feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type SSEHub struct {
	mu          sync.RWMutex
	clients     map[uint][]chan *Notification
	db          *gorm.DB
	cache       *rediscache.Client
	accountRepo *account.AccountRepository
}

func NewSSEHub(db *gorm.DB, cache *rediscache.Client, accountRepo *account.AccountRepository) *SSEHub {
	return &SSEHub{clients: make(map[uint][]chan *Notification), db: db, cache: cache, accountRepo: accountRepo}
}

func (h *SSEHub) Push(userID uint, n *Notification) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	chs, ok := h.clients[userID]
	if !ok {
		return
	}
	for _, ch := range chs {
		select {
		case ch <- n:
		default:
		}
	}
}

func (h *SSEHub) Subscribe(userID uint) chan *Notification {
	ch := make(chan *Notification, 20)
	h.mu.Lock()
	h.clients[userID] = append(h.clients[userID], ch)
	h.mu.Unlock()
	return ch
}

func (h *SSEHub) Unsubscribe(userID uint, ch chan *Notification) {
	h.mu.Lock()
	defer h.mu.Unlock()
	chs := h.clients[userID]
	for i, c := range chs {
		if c == ch {
			chs = append(chs[:i], chs[i+1:]...)
			if len(chs) == 0 {
				delete(h.clients, userID)
			} else {
				h.clients[userID] = chs
			}
			close(c)
			return
		}
	}
}

func sseAccountID(c *gin.Context) (uint, bool) {
	accountID, ok := c.Get("accountID")
	if !ok {
		return 0, false
	}
	userID, ok := accountID.(uint)
	return userID, ok && userID != 0
}

func (h *SSEHub) SSERequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.Query("token")
		if token == "" {
			token = c.GetHeader("Authorization")
			if len(token) > 7 && token[:7] == "Bearer " {
				token = token[7:]
			}
		}
		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}
		// 修复要点：旧实现只 ParseToken，不查吊销 —— 登出/改密后旧 token 仍能收推送，
		// 直到 access token 自然过期。这里复用 HTTP 中间件同一套判活逻辑。
		claims, ok := jwt.VerifyToken(c.Request.Context(), token, h.accountRepo, h.cache)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or revoked token"})
			return
		}
		c.Set("accountID", claims.AccountID)
		c.Set("sseToken", token)
		c.Next()
	}
}

// stillAuthorized 复查长连接持有的凭证。
//
// 长连接不能只靠握手那一次校验：token 过期、登出、改密、顶号之后，
// 已建立的连接必须被主动断掉，否则它就是一条绕过吊销的旁路。
// 因此 SSE 连接的最长寿命 = access token 的剩余寿命，客户端必须用新 token 重连。
func (h *SSEHub) stillAuthorized(ctx context.Context, c *gin.Context) bool {
	raw, ok := c.Get("sseToken")
	if !ok {
		return false
	}
	token, ok := raw.(string)
	if !ok || token == "" {
		return false
	}
	_, valid := jwt.VerifyToken(ctx, token, h.accountRepo, h.cache)
	return valid
}

func (h *SSEHub) SSEHandler(c *gin.Context) {
	userID, ok := sseAccountID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid account"})
		return
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.WriteHeader(http.StatusOK)

	ch := h.Subscribe(userID)
	defer h.Unsubscribe(userID, ch)

	ctx := c.Request.Context()
	flusher, _ := c.Writer.(http.Flusher)

	// 用 Ticker 而不是 time.After：通知密集时 time.After 每轮都重置，
	// 复查会被无限推迟，等于没做。
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case n, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(n)
			fmt.Fprintf(c.Writer, "data: %s\n\n", b)
			if flusher != nil {
				flusher.Flush()
			}
		case <-ticker.C:
			if !h.stillAuthorized(ctx, c) {
				return
			}
			fmt.Fprintf(c.Writer, ": keepalive\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

func (h *SSEHub) ListHandler(c *gin.Context) {
	userID, ok := sseAccountID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid account"})
		return
	}

	var notifications []Notification
	if err := h.db.WithContext(c.Request.Context()).
		Where("recipient_id = ?", userID).
		Order("created_at desc").
		Limit(50).
		Find(&notifications).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if notifications == nil {
		notifications = []Notification{}
	}
	c.JSON(200, gin.H{"notifications": notifications})
}

func (h *SSEHub) MarkReadHandler(c *gin.Context) {
	userID, ok := sseAccountID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid account"})
		return
	}

	var req struct {
		ID *uint `json:"id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var err error
	if req.ID != nil {
		err = h.db.WithContext(c.Request.Context()).Model(&Notification{}).Where("id = ? AND recipient_id = ?", *req.ID, userID).Update("is_read", true).Error
	} else {
		err = h.db.WithContext(c.Request.Context()).Model(&Notification{}).Where("recipient_id = ?", userID).Update("is_read", true).Error
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "ok"})
}

func (h *SSEHub) UnreadCountHandler(c *gin.Context) {
	userID, ok := sseAccountID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid account"})
		return
	}

	var count int64
	if err := h.db.WithContext(c.Request.Context()).Model(&Notification{}).Where("recipient_id = ? AND is_read = ?", userID, false).Count(&count).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"count": count})
}

func (h *SSEHub) RegisterRoutes(r *gin.Engine, group *gin.RouterGroup) {
	group.GET("/stream", h.SSEHandler)
	group.POST("/list", h.ListHandler)
	group.POST("/markRead", h.MarkReadHandler)
	group.POST("/unreadCount", h.UnreadCountHandler)
}

var _ NotificationHub = (*SSEHub)(nil)
