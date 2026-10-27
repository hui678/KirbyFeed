package video

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

const (
	// sessionTTL 上传会话（元数据 + 位图）的存活时间。
	sessionTTL = 24 * time.Hour
	// resultTTL 合并结果的缓存时间：让重复的 complete 返回同一个 URL，而不是 404。
	resultTTL = time.Hour
	// mergeLockTTL 合并互斥锁的租约，防止并发 complete 各合并出一份文件。
	mergeLockTTL = 10 * time.Minute
)

var errChunkCacheUnavailable = errors.New("chunk upload requires redis")

// chunkUploadResult 是 complete 成功后的幂等结果，缓存 resultTTL 时长。
type chunkUploadResult struct {
	URL     string `json:"url"`
	PlayURL string `json:"play_url"`
}

type ChunkUploadHandler struct {
	cache *rediscache.Client
}

func NewChunkUploadHandler(cache *rediscache.Client) *ChunkUploadHandler {
	return &ChunkUploadHandler{cache: cache}
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

// ── 位图读写 ──

// tmpDir / chunkPath 是临时分片目录的唯一路径规则。
// 三个 handler 必须共用它，否则「位图算一套、磁盘算另一套」的不一致会先从这里开始。
func (h *ChunkUploadHandler) tmpDir(uploadID string) string {
	return filepath.Join(".run", "uploads", "tmp", uploadID)
}

func (h *ChunkUploadHandler) chunkPath(uploadID string, index int) string {
	return filepath.Join(h.tmpDir(uploadID), fmt.Sprintf("%d", index))
}

// reconcileBits 是「位图」与「磁盘」这两份事实的唯一收敛点。
//
// 位图第 i 位为 1，不等于磁盘上那份字节还在：tmp 目录可能被清理、容器可能重建，
// 多实例部署时 upload 与 complete 还可能落在不同机器的磁盘上。只信位图的话，
// 重试会被幂等短路掉（不重建文件），complete 则永远报 chunk missing，
// 整个会话卡死到 24h TTL 过期为止。
//
// 所以任何对外表达进度的出口（init / status / complete）都必须先过这里：
// 位为 1 但磁盘上没有该片（或大小不符）的，当场清位，退回「未上传」等待客户端重传。
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
			bits, bitsErr := h.reconcileBits(c.Request.Context(), session)
			if bitsErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read upload progress"})
				return
			}
			// 续传时刷新 TTL，避免长时间未完成的会话中途过期
			_ = h.cache.PExpire(c.Request.Context(), h.bitsKey(session.UploadID), sessionTTL)
			_ = h.saveSession(c, session)
			c.JSON(http.StatusOK, gin.H{
				"upload_id":       session.UploadID,
				"uploaded_chunks": uploadedIndices(bits),
				"total_chunks":    session.TotalChunks,
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

	// 分片长度必须与该下标的期望长度一致（最后一片可短一些），防止越界/截断写入
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

	// ② 先写 .part 再原子 rename：并发的 complete 永远不会读到「写了一半」的分片。
	//    （os.Create 是 truncate 写，直接写目标文件会留下一个短暂的半片窗口）
	partPath := chunkPath + ".part"
	_ = os.Remove(partPath)
	if err := writeFileAtomic(partPath, chunkPath, chunkFile); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save chunk"})
		return
	}

	// ③ 先落盘后置位：宁可让客户端重传一片，也绝不出现「位图说传了、磁盘却没有」。
	//    SETBIT 是单 bit 原子写，不会覆盖同一会话其它分片的置位结果。
	if err := h.cache.SetBitWithTTL(c.Request.Context(), h.bitsKey(req.UploadID), int64(req.ChunkIndex), 1, sessionTTL); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update upload progress"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"chunk_index": req.ChunkIndex})
}

// writeFileAtomic 把 src 写入 partPath 后 rename 成 finalPath。
func writeFileAtomic(partPath, finalPath string, src io.Reader) error {
	dst, err := os.Create(partPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		os.Remove(partPath)
		return err
	}
	if err := dst.Close(); err != nil {
		os.Remove(partPath)
		return err
	}
	if err := os.Rename(partPath, finalPath); err != nil {
		// Windows 的 rename 不能覆盖已存在文件，先删再改一次
		if remErr := os.Remove(finalPath); remErr == nil {
			if retryErr := os.Rename(partPath, finalPath); retryErr == nil {
				return nil
			}
		}
		os.Remove(partPath)
		return err
	}
	return nil
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
	bits, err := h.reconcileBits(c.Request.Context(), session)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read upload progress"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"upload_id":       session.UploadID,
		"uploaded_chunks": uploadedIndices(bits),
		"total_chunks":    session.TotalChunks,
		"completed":       countBits(bits) == session.TotalChunks,
	})
}

// ── 接口 4：complete（按顺序合并 + 整文件校验 + 清理）──

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

	// ① 重复 complete：直接回放上次结果，而不是 404
	if res, err := h.getResult(c, req.UploadID); err == nil && res != nil {
		c.JSON(http.StatusOK, gin.H{"url": res.URL, "play_url": res.PlayURL})
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

	// ② 合并互斥：两个并发 complete 各拼一份文件会产生两个 URL
	lockKey := h.lockKey(req.UploadID)
	acquired, err := h.cache.SetNX(ctx, lockKey, "1", mergeLockTTL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to acquire merge lock"})
		return
	}
	if !acquired {
		c.JSON(http.StatusConflict, gin.H{"error": "merge already in progress"})
		return
	}
	defer func() { _ = h.cache.Del(ctx, lockKey) }()

	// 拿到锁后重新读一次，避免用加锁前的旧快照判断
	session, err = h.getSession(c, req.UploadID)
	if err != nil {
		// 会话可能刚被另一个并发 complete 清理掉，此时结果缓存里应当已经有 URL
		if res, resErr := h.getResult(c, req.UploadID); resErr == nil && res != nil {
			c.JSON(http.StatusOK, gin.H{"url": res.URL, "play_url": res.PlayURL})
			return
		}
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

	date := time.Now().Format("20060102")
	relDir := filepath.Join("videos", fmt.Sprintf("%d", accountID), date)
	root := filepath.Join(".run", "uploads")
	absDir := filepath.Join(root, relDir)
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create output dir"})
		return
	}

	filename, err := randHex(16)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate filename"})
		return
	}
	finalPath := filepath.Join(absDir, filename+".mp4")

	tmpDir := h.tmpDir(req.UploadID)

	urlPath := fmt.Sprintf("/static/videos/%d/%s/%s.mp4", accountID, date, filename)
	playURL := buildAbsoluteURL(c, urlPath)

	// ③ 按 0 → N-1 顺序合并，同时流式算整文件 MD5
	actualFileHash, missing, err := mergeChunks(session, tmpDir, finalPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":          err.Error(),
			"missing_chunks": missing,
		})
		return
	}
	if len(missing) > 0 {
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

	// ④ 整文件 MD5 必须等于 init 时声明的 file_hash，否则说明中间出了坏字节
	if !strings.EqualFold(actualFileHash, session.FileHash) {
		os.Remove(finalPath)
		c.JSON(http.StatusBadRequest, gin.H{
			"error":    "file hash mismatch",
			"expected": session.FileHash,
			"actual":   actualFileHash,
		})
		return
	}

	// ⑤ 清理临时分片
	_ = os.RemoveAll(tmpDir)

	// ⑥ 先写结果再删会话：这样重复 complete 命中结果缓存，不会二次合并
	res := &chunkUploadResult{URL: playURL, PlayURL: playURL}
	_ = h.saveResult(c, req.UploadID, res)
	_ = h.cache.Del(ctx, h.bitsKey(req.UploadID))
	_ = h.cache.Del(ctx, h.sessionKey(req.UploadID))
	_ = h.cache.Del(ctx, h.hashKey(accountID, session.FileHash))

	c.JSON(http.StatusOK, gin.H{
		"url":      playURL,
		"play_url": playURL,
	})
}

// mergeChunks 按序把临时分片拼成完整文件，顺带流式算出整文件 MD5。
//
// missing 是需要客户端补传的分片下标。合并前先把所有分片检查一遍，
// 一次性收集全部缺失/长度不符的片——而不是拼到第一片坏了就 return：
// 那样客户端只知道一个片号，只能一轮一轮地猜，而且白写了一份半成品文件。
func mergeChunks(session *ChunkUploadSession, tmpDir, finalPath string) (fileHash string, missing []int, err error) {
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

	finalFile, err := os.Create(finalPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create final file")
	}

	ok := false
	defer func() {
		if !ok {
			finalFile.Close()
			os.Remove(finalPath)
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

	if err := finalFile.Close(); err != nil {
		return "", nil, fmt.Errorf("failed to merge chunks")
	}
	ok = true
	return hex.EncodeToString(whole.Sum(nil)), nil, nil
}
