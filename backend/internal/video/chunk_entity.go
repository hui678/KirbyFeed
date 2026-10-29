package video

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	// ChunkSize 是单片上限，也是前端默认单片大小（5MB）。后端拒绝比它更大的分片。
	ChunkSize = 5 << 20 // 5 MB
	// MaxFileSize 与单次上传接口 /video/uploadVideo 的 200MB 上限保持一致。
	MaxFileSize = 200 << 20
	// MaxTotalChunks 限制单个会话能声明的片数，防止恶意客户端让服务端分配超大位图。
	MaxTotalChunks = 4096
)

// ChunkUploadSession 是上传会话的元数据。
//
// 注意：它不再携带 UploadedBits。「哪些分片已传」存在独立的 Redis 位图里
// （key: chunk_upload_bits:{upload_id}），因为 SETBIT 是原子的单 bit 写，
// 而「读整个 JSON → 改一位 → 写回」在并发上传时会互相覆盖。
type ChunkUploadSession struct {
	UploadID    string `json:"upload_id"`
	AccountID   uint   `json:"account_id"`
	Filename    string `json:"filename"`
	FileSize    int64  `json:"file_size"`
	ChunkSize   int64  `json:"chunk_size"`
	TotalChunks int    `json:"total_chunks"`
	FileHash    string `json:"file_hash"`

	// OSS 分片直传相关（仅当后端支持 multipart 时填充）。
	// OSSUploadID 非空即表示本次会话走「客户端直传 OSS + 服务端拷贝合并」模式；
	// FinalKey 是合并后最终对象的 key（同时落库为 video.play_url）。
	OSSUploadID string `json:"oss_upload_id,omitempty"`
	FinalKey    string `json:"final_key,omitempty"`
}

// ExpectedChunkSize 返回第 index 片应有的精确字节数；越界或参数非法返回 -1。
// 最后一片通常小于 ChunkSize，其余恒等于 ChunkSize。
func (s *ChunkUploadSession) ExpectedChunkSize(index int) int64 {
	if s == nil || index < 0 || index >= s.TotalChunks || s.ChunkSize <= 0 {
		return -1
	}
	remaining := s.FileSize - int64(index)*s.ChunkSize
	if remaining < 0 {
		return -1
	}
	if remaining > s.ChunkSize {
		return s.ChunkSize
	}
	return remaining
}

// Matches 判断已有会话是否与本次 init 参数一致。
// 不一致（例如客户端换了分片大小）说明是陈旧会话，不能复用。
func (s *ChunkUploadSession) Matches(req InitChunkUploadRequest) bool {
	if s == nil {
		return false
	}
	return s.FileSize == req.FileSize &&
		s.ChunkSize == req.ChunkSize &&
		s.TotalChunks == req.TotalChunks &&
		strings.EqualFold(s.FileHash, req.FileHash)
}

type InitChunkUploadRequest struct {
	Filename    string `json:"filename" binding:"required"`
	FileSize    int64  `json:"file_size" binding:"required,min=1"`
	ChunkSize   int64  `json:"chunk_size" binding:"required,min=1"`
	TotalChunks int    `json:"total_chunks" binding:"required,min=1"`
	FileHash    string `json:"file_hash" binding:"required"`
}

// Validate 校验参数自洽性：file_size / chunk_size / total_chunks 三者必须满足
// total_chunks == ceil(file_size / chunk_size)，否则客户端可以造出永远完不成的会话，
// 或在 complete 时按越界下标读文件。
func (r *InitChunkUploadRequest) Validate() error {
	if r.FileSize <= 0 || r.FileSize > MaxFileSize {
		return fmt.Errorf("file_size must be between 1 and %d bytes", MaxFileSize)
	}
	if r.ChunkSize <= 0 || r.ChunkSize > ChunkSize {
		return fmt.Errorf("chunk_size must be between 1 and %d bytes", ChunkSize)
	}
	if r.TotalChunks <= 0 || r.TotalChunks > MaxTotalChunks {
		return fmt.Errorf("total_chunks must be between 1 and %d", MaxTotalChunks)
	}
	if want := int((r.FileSize + r.ChunkSize - 1) / r.ChunkSize); want != r.TotalChunks {
		return fmt.Errorf("total_chunks mismatch: file_size=%d chunk_size=%d implies %d", r.FileSize, r.ChunkSize, want)
	}
	if len(r.FileHash) != 32 {
		return errors.New("file_hash must be a 32-character md5 hex string")
	}
	if _, err := hex.DecodeString(r.FileHash); err != nil {
		return errors.New("file_hash must be a 32-character md5 hex string")
	}
	if strings.ContainsAny(r.Filename, `/\`) || r.Filename == ".." {
		return errors.New("filename must not contain path separators")
	}
	return nil
}

type UploadChunkRequest struct {
	UploadID   string `form:"upload_id" binding:"required"`
	ChunkIndex int    `form:"chunk_index" binding:"min=0"`
	ChunkHash  string `form:"chunk_hash" binding:"required"`
}

type ChunkStatusRequest struct {
	UploadID string `json:"upload_id" binding:"required"`
}

type CompleteChunkUploadRequest struct {
	UploadID string `json:"upload_id" binding:"required"`
}
