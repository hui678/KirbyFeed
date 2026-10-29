package storage

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// ossStorage 是阿里云 OSS 后端实现。
//
// 分片上传采用「客户端直传 + 服务端拷贝合并」模式：
//  1. InitMultipart 向 OSS 申请 uploadId；
//  2. 每个分片由客户端用 SignChunkUploadURL 签发的预签名 PUT URL 直接传到 OSS
//     （key = chunks/{uploadId}/{index}），不经过 API，带宽压力全在 OSS 侧；
//  3. CompleteMultipart 用 UploadPartCopy 把每个分片对象「服务端拷贝」成最终对象的分片，
//     再 CompleteMultipartUpload，最后删掉所有分片临时对象。
// 这样 API 完全不碰视频字节，合并也只是 OSS 内部的元数据操作，没有 200MB 数据搬运。
type ossStorage struct {
	client      *oss.Client
	bucket      *oss.Bucket
	bucketName  string
	presignTTL  time.Duration
	chunkPrefix string
}

// NewOSS 创建阿里云 OSS 存储。endpoint 形如 https://oss-cn-hangzhou.aliyuncs.com。
func NewOSS(endpoint, bucketName, accessKey, secretKey string, presignTTL time.Duration) (Storage, error) {
	if endpoint == "" || bucketName == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("oss: endpoint/bucket/ak/sk must all be provided")
	}
	client, err := oss.New(endpoint, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("oss new client: %w", err)
	}
	b, err := client.Bucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("oss get bucket: %w", err)
	}
	if presignTTL <= 0 {
		presignTTL = time.Hour
	}
	return &ossStorage{
		client:      client,
		bucket:      b,
		bucketName:  bucketName,
		presignTTL:  presignTTL,
		chunkPrefix: "chunks",
	}, nil
}

func (s *ossStorage) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	opts := []oss.Option{}
	if contentType != "" {
		opts = append(opts, oss.ContentType(contentType))
	}
	if err := s.bucket.PutObject(key, r, opts...); err != nil {
		return fmt.Errorf("oss put %s: %w", key, err)
	}
	return nil
}

func (s *ossStorage) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	body, err := s.bucket.GetObject(key)
	if err != nil {
		return nil, fmt.Errorf("oss get %s: %w", key, err)
	}
	return body, nil
}

func (s *ossStorage) Delete(ctx context.Context, key string) error {
	if err := s.bucket.DeleteObject(key); err != nil {
		return fmt.Errorf("oss delete %s: %w", key, err)
	}
	return nil
}

// Resolve 返回带过期时间的预签名 GET URL（私有桶读取）。
func (s *ossStorage) Resolve(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", nil
	}
	url, err := s.bucket.SignURL(key, oss.HTTPGet, int64(s.presignTTL.Seconds()))
	if err != nil {
		return "", fmt.Errorf("oss sign get %s: %w", key, err)
	}
	return url, nil
}

func (s *ossStorage) SupportsMultipart() bool { return true }

func (s *ossStorage) InitMultipart(ctx context.Context, finalKey, contentType string) (string, error) {
	opts := []oss.Option{}
	if contentType != "" {
		opts = append(opts, oss.ContentType(contentType))
	}
	imur, err := s.bucket.InitiateMultipartUpload(finalKey, opts...)
	if err != nil {
		return "", fmt.Errorf("oss initiate multipart %s: %w", finalKey, err)
	}
	return imur.UploadID, nil
}

func (s *ossStorage) ChunkObjectKey(uploadID string, chunkIndex int) string {
	return fmt.Sprintf("%s/%s/%d", s.chunkPrefix, uploadID, chunkIndex)
}

// SignChunkUploadURL 给客户端签发「直接 PUT 这一片」的预签名 URL。
// 客户端拿到后自己把分片字节 PUT 上去，API 不再经手视频数据。
func (s *ossStorage) SignChunkUploadURL(ctx context.Context, uploadID string, chunkIndex int, ttl time.Duration) (string, error) {
	key := s.ChunkObjectKey(uploadID, chunkIndex)
	secs := int64(ttl.Seconds())
	if secs <= 0 {
		secs = int64(s.presignTTL.Seconds())
	}
	url, err := s.bucket.SignURL(key, oss.HTTPPut, secs, oss.ContentType("application/octet-stream"))
	if err != nil {
		return "", fmt.Errorf("oss sign put %s: %w", key, err)
	}
	return url, nil
}

func (s *ossStorage) ChunkExists(ctx context.Context, uploadID string, chunkIndex int) (bool, error) {
	ok, err := s.bucket.IsObjectExist(s.ChunkObjectKey(uploadID, chunkIndex))
	if err != nil {
		return false, fmt.Errorf("oss exists %s: %w", s.ChunkObjectKey(uploadID, chunkIndex), err)
	}
	return ok, nil
}

// CompleteMultipart 把 chunks/{uploadId}/{i} 这几个分片对象，通过 UploadPartCopy
// 逐个拷成最终对象的分片，再 Complete，最后删除分片临时对象。
func (s *ossStorage) CompleteMultipart(ctx context.Context, finalKey, uploadID string, totalChunks int) (string, error) {
	imur := oss.InitiateMultipartUploadResult{
		Bucket:   s.bucketName,
		Key:      finalKey,
		UploadID: uploadID,
	}

	parts := make([]oss.UploadPart, 0, totalChunks)
	for i := 0; i < totalChunks; i++ {
		srcKey := s.ChunkObjectKey(uploadID, i)
		meta, err := s.bucket.GetObjectMeta(srcKey)
		if err != nil {
			s.abortAndCleanup(imur, totalChunks)
			return "", fmt.Errorf("oss get chunk meta %s: %w", srcKey, err)
		}
		size, perr := strconv.ParseInt(meta.Get("Content-Length"), 10, 64)
		if perr != nil {
			s.abortAndCleanup(imur, totalChunks)
			return "", fmt.Errorf("oss chunk %s content-length: %w", srcKey, perr)
		}
		part, err := s.bucket.UploadPartCopy(imur, s.bucketName, srcKey, 0, size, i+1)
		if err != nil {
			s.abortAndCleanup(imur, totalChunks)
			return "", fmt.Errorf("oss upload part copy %s: %w", srcKey, err)
		}
		parts = append(parts, part)
	}

	// OSS 要求分片按 PartNumber 升序。
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	if _, err := s.bucket.CompleteMultipartUpload(imur, parts); err != nil {
		s.abortAndCleanup(imur, totalChunks)
		return "", fmt.Errorf("oss complete multipart %s: %w", finalKey, err)
	}

	// 合并成功后清理分片临时对象（失败不影响成品，仅留孤儿，可被 lifecycle 兜底）。
	for i := 0; i < totalChunks; i++ {
		_ = s.bucket.DeleteObject(s.ChunkObjectKey(uploadID, i))
	}
	return finalKey, nil
}

func (s *ossStorage) AbortMultipart(ctx context.Context, finalKey, uploadID string) error {
	imur := oss.InitiateMultipartUploadResult{
		Bucket:   s.bucketName,
		Key:      finalKey,
		UploadID: uploadID,
	}
	s.abortAndCleanup(imur, -1)
	return nil
}

// abortAndCleanup 取消分片上传并尽力删除分片临时对象（用于出错兜底）。
func (s *ossStorage) abortAndCleanup(imur oss.InitiateMultipartUploadResult, totalChunks int) {
	_ = s.bucket.AbortMultipartUpload(imur)
	if totalChunks < 0 {
		return
	}
	for i := 0; i < totalChunks; i++ {
		_ = s.bucket.DeleteObject(s.ChunkObjectKey(imur.UploadID, i))
	}
}

// ensure interfaces satisfied
var (
	_ Storage = (*localStorage)(nil)
	_ Storage = (*ossStorage)(nil)
)
