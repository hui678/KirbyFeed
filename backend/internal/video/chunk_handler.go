package video

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/storage"

	"github.com/gin-gonic/gin"
)

const (
	// sessionTTL 上传会话（元数据 + 位图）的存活时间。
	sessionTTL = 24 * time.Hour
	// resultTTL 合并结果的缓存时间：让重复的 complete 返回同一个 URL，而不是 404。
	resultTTL = time.Hour
	// mergeLockTTL 合并互斥锁的租约，防止并发 complete 各合并出一份文件。
	mergeLockTTL = 10 * time.Minute
	// maxConcurrentMerges 限制同时进行的合并数：每个合并都要顺序占用磁盘 IO，
	// 不限流时一次突发请求就能把磁盘打满，拖垮同进程的所有接口。
	maxConcurrentMerges = 4
	// mergeSlotWait 等不到合并并发名额时最多排队多久，超时直接让客户端退避重试。
	mergeSlotWait = 5 * time.Second
	// chunkPresignTTL 分片直传预签名 PUT URL 的有效期。
	chunkPresignTTL = 30 * time.Minute
)

const uploadsRoot = ".run/uploads"

var errChunkCacheUnavailable = errors.New("chunk upload requires redis")

// chunkUploadResult 是 complete 成功后的幂等结果，缓存 resultTTL 时长。
// 只存 object key（FinalKey），可访问 URL 在响应时现签，避免预签名 URL 落库过期。
// AccountID 是回放时的归属校验依据：只凭 upload_id 就能拿到 play_url 会越过会话鉴权。
type chunkUploadResult struct {
	AccountID uint   `json:"account_id"`
	Key       string `json:"key"`
}

type ChunkUploadHandler struct {
	cache    *rediscache.Client
	mergeSem chan struct{}
	storage  storage.Storage
}

func NewChunkUploadHandler(cache *rediscache.Client, store storage.Storage) *ChunkUploadHandler {
	return &ChunkUploadHandler{
		cache:    cache,
		mergeSem: make(chan struct{}, maxConcurrentMerges),
		storage:  store,
	}
}

// isOSS 判断会话是否走对象存储分片直传模式。
func (s *ChunkUploadSession) isOSS() bool {
	return s.OSSUploadID != ""
}

// ── Redis key ──
// chunk_upload:{upload_id}          会话元数据 JSON
// chunk_upload_bits:{upload_id}     分片位图（真实 Redis bitmap，bit i = 第 i 片已上传）
// chunk_upload_hash:{uid}:{hash}    (账号, 文件 MD5) → upload_id，断点续传的查找入口
// chunk_upload_result:{upload_id}   合并结果，让重复 complete 幂等
// chunk_upload_lock:{upload_id}     合并互斥锁

func (h *ChunkUploadHandler) sessionKey(uploadID string) string {
	return h.cache.Key("chunk_upload:%s", uploadID)
}

func (h *ChunkUploadHandler) bitsKey(uploadID string) string {
	return h.cache.Key("chunk_upload_bits:%s", uploadID)
}

func (h *ChunkUploadHandler) hashKey(accountID uint, fileHash string) string {
	return h.cache.Key("chunk_upload_hash:%d:%s", accountID, fileHash)
}

func (h *ChunkUploadHandler) resultKey(uploadID string) string {
	return h.cache.Key("chunk_upload_result:%s", uploadID)
}

func (h *ChunkUploadHandler) lockKey(uploadID string) string {
	return h.cache.Key("chunk_upload_lock:%s", uploadID)
}

// ── 会话读写 ──

func (h *ChunkUploadHandler) getSession(ctx *gin.Context, uploadID string) (*ChunkUploadSession, error) {
	if h.cache == nil {
		return nil, errChunkCacheUnavailable
	}
	b, err := h.cache.GetBytes(ctx.Request.Context(), h.sessionKey(uploadID))
	if err != nil {
		return nil, fmt.Errorf("upload session not found")
	}
	var s ChunkUploadSession
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("invalid session data")
	}
	if s.TotalChunks <= 0 || s.UploadID == "" {
		return nil, fmt.Errorf("invalid session data")
	}
	return &s, nil
}

func (h *ChunkUploadHandler) saveSession(ctx *gin.Context, s *ChunkUploadSession) error {
	if h.cache == nil {
		return errChunkCacheUnavailable
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return h.cache.SetBytes(ctx.Request.Context(), h.sessionKey(s.UploadID), b, sessionTTL)
}

// touchSession 把会话、位图、hash 索引一起续期。
// 三者的寿命必须一致：只续位图会让 session 先过期（接口 404），
// 只续 session 会让断点续传的查找入口先过期（同一文件被当成新上传）。
func (h *ChunkUploadHandler) touchSession(ctx context.Context, s *ChunkUploadSession) {
	if h.cache == nil || s == nil {
		return
	}
	_ = h.cache.PExpireMany(ctx, sessionTTL,
		h.sessionKey(s.UploadID),
		h.bitsKey(s.UploadID),
		h.hashKey(s.AccountID, s.FileHash),
	)
}

// ── 位图读写 ──

// tmpDir / chunkPath 是临时分片目录的唯一路径规则（仅本地盘模式使用）。
func (h *ChunkUploadHandler) tmpDir(uploadID string) string {
	return filepath.Join(uploadsRoot, "tmp", uploadID)
}

func (h *ChunkUploadHandler) chunkPath(uploadID string, index int) string {
	return filepath.Join(h.tmpDir(uploadID), fmt.Sprintf("%d", index))
}

// reconcileBits 是「位图」与「磁盘」这两份事实的唯一收敛点（仅本地盘模式）。
//
// 位图第 i 位为 1，不等于磁盘上那份字节还在：tmp 目录可能被清理、容器可能重建，
// 多实例部署时 upload 与 complete 还可能落在不同机器的磁盘上。只信位图的话，
// 重试会被幂等短路掉（不重建文件），complete 则永远报 chunk missing，
// 整个会话卡死到 24h TTL 过期为止。
func (h *ChunkUploadHandler) reconcileBits(ctx context.Context, s *ChunkUploadSession) ([]bool, error) {
	bits, err := h.cache.GetBits(ctx, h.bitsKey(s.UploadID), s.TotalChunks)
	if err != nil {
		return nil, err
	}
	if len(bits) != s.TotalChunks {
		return nil, fmt.Errorf("invalid session data")
	}
	for i, uploaded := range bits {
		if !uploaded {
			continue
		}
		fi, statErr := os.Stat(h.chunkPath(s.UploadID, i))
		if statErr == nil && fi.Size() == s.ExpectedChunkSize(i) {
			continue
		}
		bits[i] = false
		_ = h.cache.SetBitWithTTL(ctx, h.bitsKey(s.UploadID), int64(i), 0, sessionTTL)
	}
	return bits, nil
}

// ossChunkBits 根据对象存储里分片对象的存在性，重建「已传」布尔数组（OSS 模式）。
func (h *ChunkUploadHandler) ossChunkBits(ctx context.Context, s *ChunkUploadSession) ([]bool, error) {
	bits := make([]bool, s.TotalChunks)
	for i := 0; i < s.TotalChunks; i++ {
		ok, err := h.storage.ChunkExists(ctx, s.OSSUploadID, i)
		if err != nil {
			return nil, err
		}
		bits[i] = ok
	}
	return bits, nil
}

func uploadedIndices(bits []bool) []int {
	indices := make([]int, 0, len(bits))
	for i, uploaded := range bits {
		if uploaded {
			indices = append(indices, i)
		}
	}
	return indices
}

// missingIndices 是 uploadedIndices 的反面：给客户端的「该补哪些片」清单。
func missingIndices(bits []bool) []int {
	indices := make([]int, 0, len(bits))
	for i, uploaded := range bits {
		if !uploaded {
			indices = append(indices, i)
		}
	}
	return indices
}

func countBits(bits []bool) int {
	n := 0
	for _, b := range bits {
		if b {
			n++
		}
	}
	return n
}

func (h *ChunkUploadHandler) getResult(ctx *gin.Context, uploadID string) (*chunkUploadResult, error) {
	b, err := h.cache.GetBytes(ctx.Request.Context(), h.resultKey(uploadID))
	if err != nil {
		return nil, err
	}
	var res chunkUploadResult
	if err := json.Unmarshal(b, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (h *ChunkUploadHandler) saveResult(ctx *gin.Context, uploadID string, res *chunkUploadResult) error {
	b, err := json.Marshal(res)
	if err != nil {
		return err
	}
	return h.cache.SetBytes(ctx.Request.Context(), h.resultKey(uploadID), b, resultTTL)
}

// resolveResultURL 把结果里的 key 解析成可访问 URL（私有桶预签名 / 本地静态路径）。
func (h *ChunkUploadHandler) resolveResultURL(ctx *gin.Context, key string) string {
	if key == "" {
		return ""
	}
	u, err := h.storage.Resolve(ctx.Request.Context(), key)
	if err != nil {
		return key
	}
	return u
}

// ── 接口 1：init（建会话 / 断点续传入口）──

func (h *ChunkUploadHandler) InitChunkUpload(c *gin.Context) {
	if h.cache == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errChunkCacheUnavailable.Error()})
		return
	}

	var req InitChunkUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// 参数自洽校验：chunk_size / total_chunks / file_size 必须互相吻合
	if err := req.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 同一账号 + 同一文件 MD5 → 复用旧会话（断点续传）
	hashKey := h.hashKey(accountID, req.FileHash)
	existingID, err := h.cache.GetBytes(c.Request.Context(), hashKey)
	if err == nil && len(existingID) > 0 {
		session, sessErr := h.getSession(c, string(existingID))
		if sessErr == nil && session.Matches(req) {
			// 续传入口必须对账：位图里可能有「说传了但磁盘上没有」的坏片
			var bits []bool
			var bitsErr error
			if session.isOSS() {
				bits, bitsErr = h.ossChunkBits(c.Request.Context(), session)
			} else {
				bits, bitsErr = h.reconcileBits(c.Request.Context(), session)
			}
			if bitsErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read upload progress"})
				return
			}
			// 续传时统一续期：会话、位图、hash 索引三者一起，避免寿命错位
			_ = h.saveSession(c, session)
			h.touchSession(c.Request.Context(), session)
			c.JSON(http.StatusOK, gin.H{
				"upload_id":       session.UploadID,
				"uploaded_chunks": uploadedIndices(bits),
				"total_chunks":    session.TotalChunks,
				"oss":             session.isOSS(),
			})
			return
		}
		// 参数不一致 → 陈旧会话，落到下面新建会话（会覆盖 hash → upload_id 映射）
	}

	id, err := randHex(16)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate upload id"})
		return
	}
	uploadID := id + fmt.Sprintf("%d", time.Now().UnixNano())
	session := &ChunkUploadSession{
		UploadID:    uploadID,
		AccountID:   accountID,
		Filename:    req.Filename,
		FileSize:    req.FileSize,
		ChunkSize:   req.ChunkSize,
		TotalChunks: req.TotalChunks,
		FileHash:    req.FileHash,
	}

	// 对象存储模式：向 OSS 申请 uploadId，后续分片由客户端直传。
	if h.storage != nil && h.storage.SupportsMultipart() {
		finalKey, kErr := buildMediaKey("videos", accountID, ".mp4")
		if kErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate filename"})
			return
		}
		ossUploadID, mErr := h.storage.InitMultipart(c.Request.Context(), finalKey, "video/mp4")
		if mErr != nil {
			// OSS 不可用时退回本地盘模式（上传仍可用，只是走 API 收片）。
			log.Printf("chunk init: OSS multipart unavailable, fallback to local disk: %v", mErr)
		} else {
			session.OSSUploadID = ossUploadID
			session.FinalKey = finalKey
		}
	}

	if err := h.saveSession(c, session); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create session"})
		return
	}
	if err := h.cache.SetBytes(c.Request.Context(), hashKey, []byte(uploadID), sessionTTL); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create session"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"upload_id":       uploadID,
		"uploaded_chunks": []int{},
		"total_chunks":    session.TotalChunks,
		"oss":             session.isOSS(),
	})
}

// ── 接口 2：upload（传一片）──

func (h *ChunkUploadHandler) UploadChunk(c *gin.Context) {
	if h.cache == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errChunkCacheUnavailable.Error()})
		return
	}

	var req UploadChunkRequest
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	session, err := h.getSession(c, req.UploadID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if session.AccountID != accountID {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	// 序号合法性
	if req.ChunkIndex < 0 || req.ChunkIndex >= session.TotalChunks {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chunk_index"})
		return
	}

	// ── OSS 分片直传模式：给客户端签发「直接 PUT 这一片」的预签名 URL，API 不经手字节。
	if session.isOSS() {
		// 幂等短路：OSS 里已有这片就直接回成功，让客户端能安全重试。
		if ok, _ := h.storage.ChunkExists(c.Request.Context(), session.OSSUploadID, req.ChunkIndex); ok {
			c.JSON(http.StatusOK, gin.H{"chunk_index": req.ChunkIndex})
			return
		}
		uploadURL, signErr := h.storage.SignChunkUploadURL(c.Request.Context(), session.OSSUploadID, req.ChunkIndex, chunkPresignTTL)
		if signErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sign chunk upload url"})
			return
		}
		h.touchSession(c.Request.Context(), session)
		c.JSON(http.StatusOK, gin.H{
			"chunk_index": req.ChunkIndex,
			"upload_url":  uploadURL,
		})
		return
	}

	// ── 本地盘模式（原实现）──
	expected := session.ExpectedChunkSize(req.ChunkIndex)
	if expected < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chunk_index"})
		return
	}

	tmpDir := h.tmpDir(req.UploadID)
	chunkPath := h.chunkPath(req.UploadID, req.ChunkIndex)

	// 幂等短路：位图说已传，还要确认磁盘上的字节真的在（大小对得上）。
	// 否则（临时目录被清、容器重建等）清掉这一位，重新接收，避免「bit 说齐了但文件没了」。
	uploaded, err := h.cache.GetBit(c.Request.Context(), h.bitsKey(req.UploadID), int64(req.ChunkIndex))
	if err == nil && uploaded == 1 {
		if fi, statErr := os.Stat(chunkPath); statErr == nil && fi.Size() == expected {
			c.JSON(http.StatusOK, gin.H{"chunk_index": req.ChunkIndex})
			return
		}
		_ = h.cache.SetBitWithTTL(c.Request.Context(), h.bitsKey(req.UploadID), int64(req.ChunkIndex), 0, sessionTTL)
	}

	f, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}
	if f.Size != expected {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("invalid chunk size: got %d, want %d", f.Size, expected),
		})
		return
	}

	chunkFile, err := f.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read chunk"})
		return
	}
	defer chunkFile.Close()

	// ① 逐片校验 MD5，防坏字节
	hash := md5.New()
	if _, err := io.Copy(hash, chunkFile); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash chunk"})
		return
	}
	actualHash := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(actualHash, req.ChunkHash) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":    "chunk hash mismatch",
			"expected": req.ChunkHash,
			"actual":   actualHash,
		})
		return
	}

	if _, err := chunkFile.Seek(0, io.SeekStart); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read chunk"})
		return
	}

	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create temp dir"})
		return
	}

	// ② 先写请求独占的临时文件再原子 rename：并发的 complete 永远不会读到「写了一半」的分片。
	//    临时文件名必须每次请求唯一：同一分片的并行重试共用一个 .part 时，
	//    先完成者的 rename 会把后者正在写的文件搬走，后者的 rename 则可能失败或覆盖。
	if err := writeFileAtomic(chunkPath, chunkFile); err != nil {
		log.Printf("chunk upload %s[%d]: save failed: %v", req.UploadID, req.ChunkIndex, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save chunk"})
		return
	}

	// ③ 先落盘后置位：宁可让客户端重传一片，也绝不出现「位图说传了、磁盘却没有」。
	//    SETBIT 是单 bit 原子写，不会覆盖同一会话其它分片的置位结果。
	if err := h.cache.SetBitWithTTL(c.Request.Context(), h.bitsKey(req.UploadID), int64(req.ChunkIndex), 1, sessionTTL); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update upload progress"})
		return
	}
	// 大文件分片上传可能持续数小时：每片都续期，避免会话在位图仍有效时先过期
	h.touchSession(c.Request.Context(), session)

	c.JSON(http.StatusOK, gin.H{"chunk_index": req.ChunkIndex})
}

// writeFileAtomic 把 src 写成 finalPath：先写同目录下请求独占的临时文件，
// 落盘后 rename 替换目标。任何时刻 finalPath 要么不存在，要么是完整的字节。
func writeFileAtomic(finalPath string, src io.Reader) error {
	suffix, err := randHex(8)
	if err != nil {
		return err
	}
	partPath := fmt.Sprintf("%s.%s.part", finalPath, suffix)

	dst, err := os.Create(partPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		os.Remove(partPath)
		return err
	}
	if err := dst.Sync(); err != nil {
		dst.Close()
		os.Remove(partPath)
		return err
	}
	if err := dst.Close(); err != nil {
		os.Remove(partPath)
		return err
	}
	if err := renameReplace(partPath, finalPath); err != nil {
		os.Remove(partPath)
		return err
	}
	return nil
}

// renameReplace 用 src 原子替换 dst。
//
// POSIX 的 rename 可以直接覆盖目标，Windows 不允许。回退路径必须先确认 src 存在：
// 否则「先删 dst、再 rename 一个不存在的 src」会在目标缺失的瞬间把好文件也删掉。
func renameReplace(src, dst string) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = os.Rename(src, dst); err == nil {
			return nil
		}
		// src 不见了：绝不能为了一个不存在的源去删目标
		if _, statErr := os.Stat(src); statErr != nil {
			return err
		}
		if _, statErr := os.Stat(dst); statErr == nil {
			_ = os.Remove(dst)
		}
	}
	return err
}

// ── 接口 3：status（查询已传分片）──

func (h *ChunkUploadHandler) ChunkStatus(c *gin.Context) {
	if h.cache == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errChunkCacheUnavailable.Error()})
		return
	}

	var req ChunkStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	session, err := h.getSession(c, req.UploadID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if session.AccountID != accountID {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	// status 是客户端核对进度的唯一入口，必须对账后再回答：
	// 直接回位图的话，「位图有、磁盘没有」的分片会被当成已传，客户端永远发现不了。
	var bits []bool
	if session.isOSS() {
		bits, err = h.ossChunkBits(c.Request.Context(), session)
	} else {
		bits, err = h.reconcileBits(c.Request.Context(), session)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read upload progress"})
		return
	}
	// 客户端还在轮询进度，说明会话仍活跃：三把 key 一起续期
	h.touchSession(c.Request.Context(), session)

	c.JSON(http.StatusOK, gin.H{
		"upload_id":       session.UploadID,
		"uploaded_chunks": uploadedIndices(bits),
		"total_chunks":    session.TotalChunks,
		"completed":       countBits(bits) == session.TotalChunks,
		"oss":             session.isOSS(),
	})
}

// ── 接口 4：complete（合并 / 拷贝合并 + 校验 + 清理）──

func (h *ChunkUploadHandler) CompleteChunkUpload(c *gin.Context) {
	if h.cache == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errChunkCacheUnavailable.Error()})
		return
	}

	var req CompleteChunkUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// 账号必须在结果回放之前解析：结果缓存里带归属信息，
	// 否则任何知道 upload_id 的人都能绕开会话鉴权直接拿到别人的 play_url。
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// ① 重复 complete：直接回放上次结果，而不是 404
	if res, err := h.getResult(c, req.UploadID); err == nil && res != nil {
		if res.AccountID != accountID {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}
		url := h.resolveResultURL(c, res.Key)
		c.JSON(http.StatusOK, gin.H{"key": res.Key, "url": url, "play_url": url})
		return
	}

	session, err := h.getSession(c, req.UploadID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if session.AccountID != accountID {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	// ② OSS 模式：检查所有分片都已直传到 OSS，然后服务端拷贝合并。
	if session.isOSS() {
		bits, bErr := h.ossChunkBits(ctx, session)
		if bErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read upload progress"})
			return
		}
		done := countBits(bits)
		if done < session.TotalChunks {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":          "not all chunks uploaded",
				"missing":        session.TotalChunks - done,
				"missing_chunks": missingIndices(bits),
				"completed":      done,
				"total":          session.TotalChunks,
			})
			return
		}

		finalKey, cErr := h.storage.CompleteMultipart(ctx, session.FinalKey, session.OSSUploadID, session.TotalChunks)
		if cErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to assemble chunks: %v", cErr)})
			return
		}

		res := &chunkUploadResult{AccountID: accountID, Key: finalKey}
		if err := h.saveResult(c, req.UploadID, res); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save upload result"})
			return
		}
		_ = h.cache.Del(ctx, h.bitsKey(req.UploadID))
		_ = h.cache.Del(ctx, h.sessionKey(req.UploadID))
		_, _ = h.cache.CompareAndDelete(ctx, h.hashKey(accountID, session.FileHash), []byte(req.UploadID))

		url := h.resolveResultURL(c, finalKey)
		c.JSON(http.StatusOK, gin.H{"key": finalKey, "url": url, "play_url": url})
		return
	}

	// ③ 本地盘模式（原实现）：合并互斥、限流、流式算整文件 MD5、校验。
	// 合并互斥：两个并发 complete 各拼一份文件会产生两个 URL。
	//    锁带 token，释放时只删自己的锁；租约到期被别人接管后不会误删对方的锁。
	lockKey := h.lockKey(req.UploadID)
	token, acquired, err := h.cache.Lock(ctx, lockKey, mergeLockTTL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to acquire merge lock"})
		return
	}
	if !acquired {
		c.JSON(http.StatusConflict, gin.H{"error": "merge already in progress"})
		return
	}
	defer func() { _ = h.cache.Unlock(context.Background(), lockKey, token) }()

	// 锁租约可能短于一次大文件合并：后台心跳只在自己仍持有锁时续期。
	hbCtx, hbCancel := context.WithCancel(context.Background())
	defer hbCancel()
	go func() {
		ticker := time.NewTicker(mergeLockTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				_, _ = h.cache.RefreshLock(hbCtx, lockKey, token, mergeLockTTL)
			}
		}
	}()

	// 拿到锁后重新读：另一个并发请求可能已经完成并清理了会话
	if res, resErr := h.getResult(c, req.UploadID); resErr == nil && res != nil {
		if res.AccountID != accountID {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}
		url := h.resolveResultURL(c, res.Key)
		c.JSON(http.StatusOK, gin.H{"key": res.Key, "url": url, "play_url": url})
		return
	}
	session, err = h.getSession(c, req.UploadID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	// 判满要走 reconcileBits 而不是 BITCOUNT：先把「位图有、磁盘没有」的坏位修掉，
	// 再给出完整的缺失清单，客户端就能一次补完，不用一片一片试出来。
	bits, err := h.reconcileBits(ctx, session)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read upload progress"})
		return
	}
	done := countBits(bits)
	if done < session.TotalChunks {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":          "not all chunks uploaded",
			"missing":        session.TotalChunks - done,
			"missing_chunks": missingIndices(bits),
			"completed":      done,
			"total":          session.TotalChunks,
		})
		return
	}

	// ③ 全局限流：单会话靠锁互斥，但不同会话的合并没有上限，
	//    突发流量会同时占满磁盘 IO，把整个进程拖慢。
	if !h.acquireMergeSlot(ctx) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "too many concurrent merges, retry later"})
		return
	}
	defer h.releaseMergeSlot()

	date := time.Now().Format("20060102")
	// 最终对象 key：videos/{authorId}/{date}/{rand}.mp4（与 OSS 模式一致）。
	finalKey := fmt.Sprintf("videos/%d/%s/%s.mp4", accountID, date, randHexNoErr())
	finalPath := filepath.Join(uploadsRoot, finalKey)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create output dir"})
		return
	}

	tmpDir := h.tmpDir(req.UploadID)

	// ④ 按 0 → N-1 顺序合并，同时流式算整文件 MD5
	actualFileHash, missing, err := mergeChunks(session, tmpDir, finalPath+".part")
	if err != nil {
		os.Remove(finalPath + ".part")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":          err.Error(),
			"missing_chunks": missing,
		})
		return
	}
	if len(missing) > 0 {
		os.Remove(finalPath + ".part")
		// 这些位已经名不副实，一并清掉，让后续 init / status 不再跟着说谎
		for _, idx := range missing {
			_ = h.cache.SetBitWithTTL(ctx, h.bitsKey(session.UploadID), int64(idx), 0, sessionTTL)
		}
		c.JSON(http.StatusBadRequest, gin.H{
			"error":          "missing chunks",
			"missing":        len(missing),
			"missing_chunks": missing,
			"total":          session.TotalChunks,
		})
		return
	}

	// ⑤ 整文件 MD5 必须等于 init 时声明的 file_hash，否则说明中间出了坏字节
	if !strings.EqualFold(actualFileHash, session.FileHash) {
		os.Remove(finalPath + ".part")
		// 保留会话等于让客户端对着确定失败的数据无限重试，一直卡到 24h TTL；
		// 丢弃会话后重新 init 才能重新走一遍上传。
		h.invalidateSession(ctx, session)
		c.JSON(http.StatusBadRequest, gin.H{
			"error":    "file hash mismatch",
			"code":     "file_hash_mismatch",
			"expected": session.FileHash,
			"actual":   actualFileHash,
		})
		return
	}

	if err := os.Rename(finalPath+".part", finalPath); err != nil {
		os.Remove(finalPath + ".part")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to finalize merged file"})
		return
	}

	// ⑥ 先写结果、再删临时分片、最后删会话：
	//    顺序反过来时，进程在两步之间崩溃会让一个已合并成功的文件既无法回放、也无法续传。
	res := &chunkUploadResult{AccountID: accountID, Key: finalKey}
	if err := h.saveResult(c, req.UploadID, res); err != nil {
		os.Remove(finalPath)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save upload result"})
		return
	}
	_ = os.RemoveAll(tmpDir)
	_ = h.cache.Del(ctx, h.bitsKey(req.UploadID))
	_ = h.cache.Del(ctx, h.sessionKey(req.UploadID))
	// 只删仍指向本会话的映射：并发 re-init 可能已经建了新会话
	_, _ = h.cache.CompareAndDelete(ctx, h.hashKey(accountID, session.FileHash), []byte(req.UploadID))

	url := h.resolveResultURL(c, finalKey)
	c.JSON(http.StatusOK, gin.H{
		"key":      finalKey,
		"url":      url,
		"play_url": url,
	})
}

// randHexNoErr 是 finalKey 文件名生成，失败返回固定串（极端 unlikely）。
func randHexNoErr() string {
	s, err := randHex(16)
	if err != nil {
		return "fallback"
	}
	return s
}

func (h *ChunkUploadHandler) acquireMergeSlot(ctx context.Context) bool {
	if h.mergeSem == nil {
		return true
	}
	timer := time.NewTimer(mergeSlotWait)
	defer timer.Stop()
	select {
	case h.mergeSem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

func (h *ChunkUploadHandler) releaseMergeSlot() {
	if h.mergeSem == nil {
		return
	}
	<-h.mergeSem
}

// invalidateSession 在整文件校验失败后丢弃会话。
// 不清 hash 索引会让同一文件重新 init 时命中这个必然失败的会话。
func (h *ChunkUploadHandler) invalidateSession(ctx context.Context, s *ChunkUploadSession) {
	_ = os.RemoveAll(h.tmpDir(s.UploadID))
	_ = h.cache.Del(ctx, h.bitsKey(s.UploadID))
	_ = h.cache.Del(ctx, h.sessionKey(s.UploadID))
	_, _ = h.cache.CompareAndDelete(ctx, h.hashKey(s.AccountID, s.FileHash), []byte(s.UploadID))
}

// mergeChunks 按序把临时分片拼成完整文件，顺带流式算出整文件 MD5。
// missing 是需要客户端补传的分片下标。合并前先把所有分片检查一遍，
// 一次性收集全部缺失/长度不符的片——而不是拼到第一片坏了就 return：
// 那样客户端只知道一个片号，只能一轮一轮地猜，而且白写了一份半成品文件。
func mergeChunks(session *ChunkUploadSession, tmpDir, partPath string) (fileHash string, missing []int, err error) {
	// ① 先看一遍再动手
	for i := 0; i < session.TotalChunks; i++ {
		fi, statErr := os.Stat(filepath.Join(tmpDir, fmt.Sprintf("%d", i)))
		if statErr != nil || fi.Size() != session.ExpectedChunkSize(i) {
			missing = append(missing, i)
		}
	}
	if len(missing) > 0 {
		return "", missing, nil
	}

	finalFile, err := os.Create(partPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create final file")
	}

	ok := false
	defer func() {
		if !ok {
			finalFile.Close()
			os.Remove(partPath)
		}
	}()

	whole := md5.New()
	sink := io.MultiWriter(finalFile, whole)
	for i := 0; i < session.TotalChunks; i++ {
		cf, err := os.Open(filepath.Join(tmpDir, fmt.Sprintf("%d", i)))
		if err != nil {
			// 与 ① 之间被并发清理了，退化成「缺这一片」，交给上层让客户端补传
			return "", []int{i}, fmt.Errorf("chunk %d missing", i)
		}
		if _, err := io.Copy(sink, cf); err != nil {
			cf.Close()
			return "", nil, fmt.Errorf("failed to merge chunks")
		}
		cf.Close()
	}

	// 先刷盘再交给上层 rename：否则断电/崩溃可能留下一个「名字对、内容缺尾」的成品。
	if err := finalFile.Sync(); err != nil {
		return "", nil, fmt.Errorf("failed to flush merged file")
	}
	if err := finalFile.Close(); err != nil {
		return "", nil, fmt.Errorf("failed to merge chunks")
	}
	ok = true
	return hex.EncodeToString(whole.Sum(nil)), nil, nil
}
