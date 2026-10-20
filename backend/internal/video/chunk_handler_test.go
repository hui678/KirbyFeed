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
