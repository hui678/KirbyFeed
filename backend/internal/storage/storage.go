// Package storage 定义了统一的对象存储抽象层。
//
// 设计目标：让业务代码（上传/下载/读取 URL）不感知底层是「本地磁盘」还是「阿里云 OSS」。
//   - localStorage：沿用原 .run/uploads 目录，靠 gin 的 /static 静态服务对外提供；
//     未配置对象存储时零成本回落到它，保持原行为不变。
//   - ossStorage：阿里云 OSS（S3 风格的多分片直传也走这里），读取用私有桶 + 预签名 URL。
//
// 关键约定：数据库里只存「object key」（如 videos/1/20260101/abcd.mp4），
// 真正的可访问 URL 在「读取响应时」由 Resolve 现签（预签名 URL 会过期，
// 绝不能把签名后的整条 URL 落库）。本地模式下 key 会被解析成 /static/{key}。
package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotSupported 表示该后端不支持某个能力（如本地盘不支持 multipart）。
var ErrNotSupported = errors.New("storage: operation not supported by this backend")

// Part 标识一次 multipart 上传中的一个分片（阿里云 OSS / S3 通用）。
type Part struct {
	Number int
	ETag   string
}

// Storage 是统一的对象存储接口。
//
// 读取类方法：
//   - Put/Open/Delete：对象的写入/读取/删除。
//   - Resolve：把 key 解析成「可直接返回给客户端的可访问 URL」：
//     私有桶返回带过期的预签名 GET URL；本地盘返回 /static/{key}。
//
// 分片直传类方法（仅对象存储后端实现，本地盘返回 ErrNotSupported）：
//   - SupportsMultipart：是否支持服务端托管的分片上传。
//   - InitMultipart：向对象存储申请一个 uploadId。
//   - SignChunkUploadURL：给某个分片签发「客户端可直接 PUT」的预签名 URL。
//   - ChunkObjectKey / ChunkExists：分片落盘后（在对象存储里）的 key 与存在性检查。
//   - CompleteMultipart：把所有分片「服务端拷贝合并」成最终对象并清理分片。
//   - AbortMultipart：放弃并清理分片。
type Storage interface {
	// Put 写入一个对象。size<0 时由实现自行读取（OSS 允许流式）。
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	// Open 打开对象内容用于下载/转发。
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete 删除对象。
	Delete(ctx context.Context, key string) error
	// Resolve 返回 key 对应的可访问 URL（预签名 GET，或本地静态路径）。
	Resolve(ctx context.Context, key string) (string, error)

	// ── 分片直传（仅对象存储支持）──
	SupportsMultipart() bool
	// InitMultipart 为 finalKey 发起一次分片上传，返回后端 uploadId。
	InitMultipart(ctx context.Context, finalKey, contentType string) (uploadID string, err error)
	// SignChunkUploadURL 给第 chunkIndex 片签发客户端可直接 PUT 的预签名 URL。
	SignChunkUploadURL(ctx context.Context, uploadID string, chunkIndex int, ttl time.Duration) (string, error)
	// ChunkObjectKey 返回某次上传中第 chunkIndex 片在对象存储里的 key。
	ChunkObjectKey(uploadID string, chunkIndex int) string
	// ChunkExists 判断第 chunkIndex 片是否已经上传到对象存储。
	ChunkExists(ctx context.Context, uploadID string, chunkIndex int) (bool, error)
	// CompleteMultipart 把所有分片合并成 finalKey 指向的最终对象，并清理分片临时对象。
	CompleteMultipart(ctx context.Context, finalKey, uploadID string, totalChunks int) (string, error)
	// AbortMultipart 取消分片上传并清理分片临时对象。
	AbortMultipart(ctx context.Context, finalKey, uploadID string) error
}

// IsNotSupported 判断 err 是否「后端不支持」。
func IsNotSupported(err error) bool {
	return errors.Is(err, ErrNotSupported)
}
