package http

import (
	"crypto/subtle"
	nethttp "net/http"
	"strconv"
	"time"

	"feedsystem_video_go/internal/video"

	"github.com/gin-gonic/gin"
)

// outboxAdminHandler 是 Outbox 的最小运维入口：
// 查看状态、列出 failed、把 failed 重置回 pending。
// failed 是终态，如果没有查询和重放入口，它和「静默丢弃」没有区别。
type outboxAdminHandler struct {
	repo *video.OutboxRepository
}

func newOutboxAdminHandler(repo *video.OutboxRepository) *outboxAdminHandler {
	return &outboxAdminHandler{repo: repo}
}

// adminTokenAuth 用固定 token 保护运维接口。使用常量时间比较，避免时序侧信道。
func adminTokenAuth(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		got := c.GetHeader("X-Admin-Token")
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			c.AbortWithStatusJSON(nethttp.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}

// Stats GET /admin/outbox/stats
func (h *outboxAdminHandler) Stats(c *gin.Context) {
	stats, err := h.repo.Stats(c.Request.Context(), time.Now().Add(-video.OutboxClaimTimeout))
	if err != nil {
		c.JSON(nethttp.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(nethttp.StatusOK, stats)
}

// ListFailed GET /admin/outbox/failed?limit=50
func (h *outboxAdminHandler) ListFailed(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	msgs, err := h.repo.ListFailed(c.Request.Context(), limit)
	if err != nil {
		c.JSON(nethttp.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(nethttp.StatusOK, gin.H{"count": len(msgs), "items": msgs})
}

// Replay POST /admin/outbox/replay {"ids":[1,2]} 或 {"id":1}
// 重放是安全的：event_id 保持不变，消费端按幂等键/覆盖写消化重复。
func (h *outboxAdminHandler) Replay(c *gin.Context) {
	var req struct {
		ID  uint   `json:"id"`
		IDs []uint `json:"ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(nethttp.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if req.ID != 0 {
		req.IDs = append(req.IDs, req.ID)
	}
	if len(req.IDs) == 0 {
		c.JSON(nethttp.StatusBadRequest, gin.H{"error": "id or ids is required"})
		return
	}
	if len(req.IDs) > 200 {
		c.JSON(nethttp.StatusBadRequest, gin.H{"error": "at most 200 ids per request"})
		return
	}

	n, err := h.repo.ReplayFailed(c.Request.Context(), req.IDs)
	if err != nil {
		c.JSON(nethttp.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(nethttp.StatusOK, gin.H{"replayed": n})
}
