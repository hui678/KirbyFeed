package video

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

// ── helpers ──

const testAccountID uint = 1

func setupTestEnv(t *testing.T) (*ChunkUploadHandler, func()) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}

	client := rediscache.NewClient(
		goredis.NewClient(&goredis.Options{Addr: mr.Addr()}),
		"",
	)
	handler := NewChunkUploadHandler(client)

	origDir, _ := os.Getwd()
	tmpDir, err := os.MkdirTemp("", "chunk-upload-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	cleanup := func() {
		os.Chdir(origDir)
		os.RemoveAll(tmpDir)
		client.Close()
		mr.Close()
	}
	return handler, cleanup
}

func newJSONContext(t *testing.T, path string, body interface{}) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "localhost"

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	c.Set("accountID", testAccountID)
	return c, rec
}

func newMultipartContext(t *testing.T, path string, fields map[string]string, fileContent []byte) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	for k, v := range fields {
		if err := writer.WriteField(k, v); err != nil {
			t.Fatalf("write field %s: %v", k, err)
		}
	}
	part, err := writer.CreateFormFile("file", "chunk.bin")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write(fileContent); err != nil {
		t.Fatalf("write file: %v", err)
	}
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Host = "localhost"

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	c.Set("accountID", testAccountID)
	return c, rec
}

func computeMD5(data []byte) string {
	h := md5.Sum(data)
	return hex.EncodeToString(h[:])
}

func parseJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("parse json: %v, body: %s", err, rec.Body.String())
	}
	return m
}

func makeTestChunks(t *testing.T, totalChunks, chunkSize int) ([][]byte, []string, string) {
	t.Helper()
	chunks := make([][]byte, totalChunks)
	chunkHashes := make([]string, totalChunks)
	var full bytes.Buffer
	for i := 0; i < totalChunks; i++ {
		data := make([]byte, chunkSize)
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("rand read: %v", err)
		}
		chunks[i] = data
		chunkHashes[i] = computeMD5(data)
		full.Write(data)
	}
	return chunks, chunkHashes, computeMD5(full.Bytes())
}

func initUpload(t *testing.T, h *ChunkUploadHandler, filename string, fileSize int64, chunkSize int64, totalChunks int, fileHash string) string {
	t.Helper()
	c, rec := newJSONContext(t, "/video/chunk/init", InitChunkUploadRequest{
		Filename:    filename,
		FileSize:    fileSize,
		ChunkSize:   chunkSize,
		TotalChunks: totalChunks,
		FileHash:    fileHash,
	})
	h.InitChunkUpload(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("init: expected 200, got %d, body: %s", rec.Code, rec.Body.String())
	}
	resp := parseJSON(t, rec)
	return resp["upload_id"].(string)
}

func uploadChunk(t *testing.T, h *ChunkUploadHandler, uploadID string, chunkIndex int, chunkHash string, chunkData []byte) {
	t.Helper()
	c, rec := newMultipartContext(t, "/video/chunk/upload", map[string]string{
		"upload_id":   uploadID,
		"chunk_index": fmt.Sprintf("%d", chunkIndex),
		"chunk_hash":  chunkHash,
	}, chunkData)
	h.UploadChunk(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload chunk %d: expected 200, got %d, body: %s", chunkIndex, rec.Code, rec.Body.String())
	}
}

func completeUpload(t *testing.T, h *ChunkUploadHandler, uploadID string) map[string]interface{} {
	t.Helper()
	c, rec := newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	return parseJSON(t, rec)
}

// readMergedFile extracts the file path from the response URL and reads it.
// URL format: http://localhost/static/videos/<accountID>/<date>/<name>.mp4
func readMergedFile(t *testing.T, resp map[string]interface{}) []byte {
	t.Helper()
	urlStr := resp["url"].(string)
	idx := len("http://localhost/static/")
	fsPath := filepath.Join(".run", "uploads", urlStr[idx:])
	data, err := os.ReadFile(fsPath)
	if err != nil {
		t.Fatalf("read merged file %s: %v", fsPath, err)
	}
	return data
}

// ── tests ──

func TestFullChunkUploadFlow(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 1024
	totalChunks := 3
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "test.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)

	for i := 0; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	c, rec := newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("complete: expected 200, got %d, body: %s", rec.Code, rec.Body.String())
	}
	resp := parseJSON(t, rec)
	if resp["url"] == nil || resp["play_url"] == nil {
		t.Fatal("complete response missing url or play_url")
	}

	// verify merged file content matches original chunks
	merged := readMergedFile(t, resp)
	var expected bytes.Buffer
	for _, ch := range chunks {
		expected.Write(ch)
	}
	if !bytes.Equal(merged, expected.Bytes()) {
		t.Fatalf("merged file content mismatch: got %d bytes, want %d bytes", len(merged), expected.Len())
	}

	// verify temp dir cleaned up
	tmpDir := filepath.Join(".run", "uploads", "tmp", uploadID)
	if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
		t.Fatalf("temp dir should be removed: %s", tmpDir)
	}
}

func TestBreakpointResume(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 512
	totalChunks := 4
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "resume.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)

	// upload first 2 chunks
	for i := 0; i < 2; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	// re-init with same file_hash → should resume existing session
	c, rec := newJSONContext(t, "/video/chunk/init", InitChunkUploadRequest{
		Filename:    "resume.mp4",
		FileSize:    int64(totalChunks * chunkSize),
		ChunkSize:   int64(chunkSize),
		TotalChunks: totalChunks,
		FileHash:    fileHash,
	})
	h.InitChunkUpload(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("re-init: expected 200, got %d, body: %s", rec.Code, rec.Body.String())
	}
	resp := parseJSON(t, rec)

	resumedID := resp["upload_id"].(string)
	if resumedID != uploadID {
		t.Fatalf("resume should return same upload_id: got %s, want %s", resumedID, uploadID)
	}

	chunkList := resp["uploaded_chunks"].([]interface{})
	if len(chunkList) != 2 {
		t.Fatalf("expected 2 uploaded chunks on resume, got %d", len(chunkList))
	}

	// upload remaining chunks and complete
	for i := 2; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	resp = completeUpload(t, h, uploadID)
	merged := readMergedFile(t, resp)
	var expected bytes.Buffer
	for _, ch := range chunks {
		expected.Write(ch)
	}
	if !bytes.Equal(merged, expected.Bytes()) {
		t.Fatalf("merged file mismatch after resume")
	}
}

func TestIdempotentChunkUpload(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 2
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "idempotent.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)

	// upload chunk 0 twice — both should succeed
	uploadChunk(t, h, uploadID, 0, chunkHashes[0], chunks[0])
	uploadChunk(t, h, uploadID, 0, chunkHashes[0], chunks[0])

	uploadChunk(t, h, uploadID, 1, chunkHashes[1], chunks[1])

	c, rec := newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("complete: expected 200, got %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestHashMismatch(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 1
	chunks, _, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "hashfail.mp4", int64(chunkSize), int64(chunkSize), totalChunks, fileHash)

	// upload with wrong hash
	c, rec := newMultipartContext(t, "/video/chunk/upload", map[string]string{
		"upload_id":   uploadID,
		"chunk_index": "0",
		"chunk_hash":  "deadbeef000000000000000000000000",
	}, chunks[0])
	h.UploadChunk(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for hash mismatch, got %d", rec.Code)
	}
	resp := parseJSON(t, rec)
	if resp["error"] != "chunk hash mismatch" {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
	if resp["expected"] == nil || resp["actual"] == nil {
		t.Fatal("response should contain expected and actual hash")
	}
	actualHash := computeMD5(chunks[0])
	if resp["actual"] != actualHash {
		t.Fatalf("actual hash: got %v, want %s", resp["actual"], actualHash)
	}

	// retry with correct hash should succeed
	uploadChunk(t, h, uploadID, 0, actualHash, chunks[0])
}

func TestIncompleteMerge(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 128
	totalChunks := 5
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "incomplete.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)

	// upload only 3 out of 5
	for i := 0; i < 3; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	c, rec := newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for incomplete, got %d", rec.Code)
	}
	resp := parseJSON(t, rec)
	if resp["error"] != "not all chunks uploaded" {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
	if int(resp["missing"].(float64)) != 2 {
		t.Fatalf("expected missing=2, got %v", resp["missing"])
	}
	if int(resp["completed"].(float64)) != 3 {
		t.Fatalf("expected completed=3, got %v", resp["completed"])
	}
	if int(resp["total"].(float64)) != 5 {
		t.Fatalf("expected total=5, got %v", resp["total"])
	}

	// upload remaining and retry
	for i := 3; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}
	c, rec = newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("complete after fix: expected 200, got %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestInitRejectsInconsistentParams(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	validHash := computeMD5([]byte("whatever"))

	// total_chunks 必须等于 ceil(file_size / chunk_size)
	c, rec := newJSONContext(t, "/video/chunk/init", InitChunkUploadRequest{
		Filename:    "bad.mp4",
		FileSize:    3072,
		ChunkSize:   1024,
		TotalChunks: 5,
		FileHash:    validHash,
	})
	h.InitChunkUpload(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for inconsistent total_chunks, got %d: %s", rec.Code, rec.Body.String())
	}

	// chunk_size 不得超过后端上限 5MB
	oversized := int64(ChunkSize) + 1
	c, rec = newJSONContext(t, "/video/chunk/init", InitChunkUploadRequest{
		Filename:    "big.mp4",
		FileSize:    oversized,
		ChunkSize:   oversized,
		TotalChunks: 1,
		FileHash:    validHash,
	})
	h.InitChunkUpload(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized chunk_size, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUploadRejectsWrongChunkSize(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 2
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "badsize.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)

	// 只发半片，长度校验应当拦下
	half := chunks[0][:chunkSize/2]
	c, rec := newMultipartContext(t, "/video/chunk/upload", map[string]string{
		"upload_id":   uploadID,
		"chunk_index": "0",
		"chunk_hash":  computeMD5(half),
	}, half)
	h.UploadChunk(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid chunk size, got %d: %s", rec.Code, rec.Body.String())
	}

	// 正确长度仍然可以上传
	uploadChunk(t, h, uploadID, 0, chunkHashes[0], chunks[0])
}

// TestConcurrentChunkUploadKeepsAllBits 是 P0 回归测试：
// 旧实现「读整个 session JSON → 改一位 → 写回」，并发传片会互相覆盖位图。
func TestConcurrentChunkUploadKeepsAllBits(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 128
	totalChunks := 8
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "concurrent.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)

	var wg sync.WaitGroup
	errs := make(chan error, totalChunks)
	for i := 0; i < totalChunks; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, rec := newMultipartContext(t, "/video/chunk/upload", map[string]string{
				"upload_id":   uploadID,
				"chunk_index": fmt.Sprintf("%d", idx),
				"chunk_hash":  chunkHashes[idx],
			}, chunks[idx])
			h.UploadChunk(c)
			if rec.Code != http.StatusOK {
				errs <- fmt.Errorf("chunk %d: got %d, body %s", idx, rec.Code, rec.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent upload failed: %v", err)
	}

	c, rec := newJSONContext(t, "/video/chunk/status", ChunkStatusRequest{UploadID: uploadID})
	h.ChunkStatus(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: expected 200, got %d", rec.Code)
	}
	resp := parseJSON(t, rec)
	list, _ := resp["uploaded_chunks"].([]interface{})
	if len(list) != totalChunks {
		t.Fatalf("expected all %d bits set after concurrent upload, got %d", totalChunks, len(list))
	}
	if resp["completed"] != true {
		t.Fatalf("expected completed=true, got %v", resp["completed"])
	}
}

func TestCompleteIsIdempotent(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 2
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "replay.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)
	for i := 0; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	first := completeUpload(t, h, uploadID)
	url1, ok := first["url"].(string)
	if !ok || url1 == "" {
		t.Fatalf("complete response missing url: %v", first)
	}

	// 会话已清理，第二次 complete 必须回放同一 URL，而不是 404
	c, rec := newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("replay complete: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	second := parseJSON(t, rec)
	if second["url"] != url1 {
		t.Fatalf("replay should return same url, got %v want %v", second["url"], url1)
	}
}

func TestCompleteRejectsWrongFileHash(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 128
	totalChunks := 2
	chunks, chunkHashes, _ := makeTestChunks(t, totalChunks, chunkSize)

	// 声明一个与真实内容不符的整文件 MD5：单片校验会通过，合并后必须被拦下
	wrongHash := computeMD5([]byte("definitely-not-this-file"))
	uploadID := initUpload(t, h, "wronghash.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, wrongHash)
	for i := 0; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	c, rec := newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for file hash mismatch, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := parseJSON(t, rec)
	if resp["error"] != "file hash mismatch" {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
}

func TestChunkStatus(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 3
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "status.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)

	assertStatus := func(wantCount int) {
		t.Helper()
		c, rec := newJSONContext(t, "/video/chunk/status", ChunkStatusRequest{UploadID: uploadID})
		h.ChunkStatus(c)
		if rec.Code != http.StatusOK {
			t.Fatalf("status: expected 200, got %d", rec.Code)
		}
		resp := parseJSON(t, rec)
		list, _ := resp["uploaded_chunks"].([]interface{})
		if len(list) != wantCount {
			t.Fatalf("expected %d uploaded chunks, got %d", wantCount, len(list))
		}
		if int(resp["total_chunks"].(float64)) != totalChunks {
			t.Fatalf("expected total_chunks=%d", totalChunks)
		}
	}

	assertStatus(0)

	uploadChunk(t, h, uploadID, 0, chunkHashes[0], chunks[0])
	assertStatus(1)

	uploadChunk(t, h, uploadID, 1, chunkHashes[1], chunks[1])
	uploadChunk(t, h, uploadID, 2, chunkHashes[2], chunks[2])
	assertStatus(3)
}

// ── 「位图说传了、磁盘却没有」的自愈（P0）──
// 位图和磁盘是两份独立的事实：tmp 目录被清理、容器重建、多实例磁盘不共享，
// 都会让位图开始说谎。对外表达进度的每个出口都必须先对账，否则会话会卡死到 TTL 过期。

func intList(t *testing.T, raw interface{}) []int {
	t.Helper()
	arr, ok := raw.([]interface{})
	if !ok {
		t.Fatalf("expected list, got %#v", raw)
	}
	out := make([]int, 0, len(arr))
	for _, v := range arr {
		f, ok := v.(float64)
		if !ok {
			t.Fatalf("expected number in list, got %#v", v)
		}
		out = append(out, int(f))
	}
	return out
}

func removeChunkFile(t *testing.T, uploadID string, index int) {
	t.Helper()
	path := filepath.Join(".run", "uploads", "tmp", uploadID, fmt.Sprintf("%d", index))
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove chunk file %s: %v", path, err)
	}
}

// TestStatusRepairsBitsWhenChunkFileLost：status 是客户端核对进度的入口，
// 它必须说实话——哪怕位图自己不同意。
func TestStatusRepairsBitsWhenChunkFileLost(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 3
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "lost.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)
	for i := 0; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	// 模拟 tmp 被清理：位图仍声称三片都在，磁盘上第 1 片已消失
	removeChunkFile(t, uploadID, 1)

	c, rec := newJSONContext(t, "/video/chunk/status", ChunkStatusRequest{UploadID: uploadID})
	h.ChunkStatus(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: expected 200, got %d", rec.Code)
	}
	resp := parseJSON(t, rec)

	got := intList(t, resp["uploaded_chunks"])
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("expected [0 2] after chunk 1 lost, got %v", got)
	}
	if resp["completed"] != false {
		t.Fatalf("completed must be false when a chunk is gone, got %v", resp["completed"])
	}
}

// TestInitResumptionReportsTruthfulProgress：用户刷新页面后重选同一文件是最常见的
// 续传场景。init 如果对账不实，会返回一个「已传」实际却丢失的片，
// 客户端跳过它 → complete 永久失败。
func TestInitResumptionReportsTruthfulProgress(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 3
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "resume-lost.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)
	for i := 0; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}
	removeChunkFile(t, uploadID, 1)

	// 重新选同一个文件 → 命中同一会话
	c, rec := newJSONContext(t, "/video/chunk/init", InitChunkUploadRequest{
		Filename:    "resume-lost.mp4",
		FileSize:    int64(totalChunks * chunkSize),
		ChunkSize:   int64(chunkSize),
		TotalChunks: totalChunks,
		FileHash:    fileHash,
	})
	h.InitChunkUpload(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("re-init: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := parseJSON(t, rec)
	if resp["upload_id"] != uploadID {
		t.Fatalf("expected same upload_id %s, got %v", uploadID, resp["upload_id"])
	}

	got := intList(t, resp["uploaded_chunks"])
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("resumption must not claim chunk 1 uploaded, got %v", got)
	}
}

// TestCompleteReportsAllMissingChunksAndRecovers：合并失败时必须一次性给出
// 全部缺失片，客户端才能一轮补完；补传后必须能真正合并成功（自愈闭环）。
func TestCompleteReportsAllMissingChunksAndRecovers(t *testing.T) {
	h, cleanup := setupTestEnv(t)
	defer cleanup()

	chunkSize := 256
	totalChunks := 4
	chunks, chunkHashes, fileHash := makeTestChunks(t, totalChunks, chunkSize)

	uploadID := initUpload(t, h, "gaps.mp4", int64(totalChunks*chunkSize), int64(chunkSize), totalChunks, fileHash)
	for i := 0; i < totalChunks; i++ {
		uploadChunk(t, h, uploadID, i, chunkHashes[i], chunks[i])
	}

	// 丢两片，且不连续：必须在一次响应里同时报出来
	removeChunkFile(t, uploadID, 0)
	removeChunkFile(t, uploadID, 3)

	c, rec := newJSONContext(t, "/video/chunk/complete", CompleteChunkUploadRequest{UploadID: uploadID})
	h.CompleteChunkUpload(c)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 with missing chunks, got %d: %s", rec.Code, rec.Body.String())
	}
	missing := intList(t, parseJSON(t, rec)["missing_chunks"])
	if len(missing) != 2 || missing[0] != 0 || missing[1] != 3 {
		t.Fatalf("expected missing [0 3], got %v", missing)
	}

	// 按清单补传后必须能合并成功——这是「会话不会卡死」的最终证明
	uploadChunk(t, h, uploadID, 0, chunkHashes[0], chunks[0])
	uploadChunk(t, h, uploadID, 3, chunkHashes[3], chunks[3])

	res := completeUpload(t, h, uploadID)
	if url, ok := res["url"].(string); !ok || url == "" {
		t.Fatalf("expected url after recovery, got %v", res)
	}
}
