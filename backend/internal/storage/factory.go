package storage

import (
	"log"
	"time"

	"feedsystem_video_go/internal/config"
)

// NewFromConfig 根据配置构建存储后端。
//
// 未启用（或 provider 非 oss）时回落到本地磁盘，保证「不配对象存储也能跑」。
// 启用了 OSS 但初始化失败（凭证错误/网络不可达）时，记录错误并回落本地盘，
// 避免整个服务起不来——上传会变成「写本地」，但仍可用，排查后再切回 OSS。
func NewFromConfig(cfg config.ObjectStorageConfig) Storage {
	if !cfg.Enabled || cfg.Provider != "oss" {
		return NewLocal(".run/uploads")
	}
	ttl := time.Duration(cfg.PresignTTL) * time.Second
	s, err := NewOSS(cfg.Endpoint, cfg.Bucket, cfg.AccessKey, cfg.SecretKey, ttl)
	if err != nil {
		log.Printf("object storage (oss) init failed, fallback to local disk: %v", err)
		return NewLocal(".run/uploads")
	}
	log.Printf("object storage enabled: oss bucket=%s endpoint=%s", cfg.Bucket, cfg.Endpoint)
	return s
}
