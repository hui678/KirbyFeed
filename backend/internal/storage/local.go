package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// localStorage 把对象存到本地磁盘 .run/uploads，靠 gin 的 /static 对外提供。
// 它是「未配置对象存储」时的默认后端，行为与改造前完全一致。
type localStorage struct {
	root string
}

// NewLocal 创建本地存储，root 为上传根目录（默认 .run/uploads）。
func NewLocal(root string) Storage {
	if root == "" {
		root = ".run/uploads"
	}
	return &localStorage{root: root}
}

func (s *localStorage) objectPath(key string) string {
	// 防目录穿越：key 里不允许出现 .. 或绝对路径。
	clean := filepath.Clean("/" + key)
	return filepath.Join(s.root, clean)
}

func (s *localStorage) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	dst := s.objectPath(key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("local storage mkdir: %w", err)
	}
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("local storage create: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("local storage write: %w", err)
	}
	return nil
}

func (s *localStorage) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	f, err := os.Open(s.objectPath(key))
	if err != nil {
		return nil, fmt.Errorf("local storage open: %w", err)
	}
	return f, nil
}

func (s *localStorage) Delete(ctx context.Context, key string) error {
	err := os.Remove(s.objectPath(key))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local storage delete: %w", err)
	}
	return nil
}

// Resolve 把 key 还原成 /static/{key} 形式的本地可访问路径。
// 兼容改造前直接落库「完整 /static/... 或 http(s):// 路径」的旧数据：原样返回。
func (s *localStorage) Resolve(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", nil
	}
	if strings.HasPrefix(key, "http://") || strings.HasPrefix(key, "https://") || strings.HasPrefix(key, "/") {
		return key, nil
	}
	return "/static/" + strings.TrimPrefix(key, "/"), nil
}

func (s *localStorage) SupportsMultipart() bool { return false }

func (s *localStorage) InitMultipart(ctx context.Context, finalKey, contentType string) (string, error) {
	return "", ErrNotSupported
}

func (s *localStorage) SignChunkUploadURL(ctx context.Context, uploadID string, chunkIndex int, ttl time.Duration) (string, error) {
	return "", ErrNotSupported
}

func (s *localStorage) ChunkObjectKey(uploadID string, chunkIndex int) string {
	return ""
}

func (s *localStorage) ChunkExists(ctx context.Context, uploadID string, chunkIndex int) (bool, error) {
	return false, ErrNotSupported
}

func (s *localStorage) CompleteMultipart(ctx context.Context, finalKey, uploadID string, totalChunks int) (string, error) {
	return "", ErrNotSupported
}

func (s *localStorage) AbortMultipart(ctx context.Context, finalKey, uploadID string) error {
	return ErrNotSupported
}
