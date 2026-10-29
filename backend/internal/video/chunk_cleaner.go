package video

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/storage"
)

const chunkCleanerInterval = time.Hour

// StartChunkCleaner 周期清理两类没有主人的残留：
//
//  1. tmp/{upload_id}：Redis 里没有会话（已过期，或会话从未建成）且目录超过 sessionTTL；
//  2. videos/**/*.mp4.part：合并中途崩溃留下的半成品。
//
// 有会话的目录一律不动：会话还活着，分片就还有续传价值，哪怕目录看起来旧。
func StartChunkCleaner(ctx context.Context, cache *rediscache.Client, interval time.Duration) {
	if cache == nil {
		return
	}
	if interval <= 0 {
		interval = chunkCleanerInterval
	}
	h := NewChunkUploadHandler(cache, storage.NewLocal(".run/uploads"))

	h.sweepStaleUploads(ctx, time.Now())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			h.sweepStaleUploads(ctx, now)
		}
	}
}

func (h *ChunkUploadHandler) sweepStaleUploads(ctx context.Context, now time.Time) {
	cutoff := now.Add(-sessionTTL)

	tmpRoot := filepath.Join(uploadsRoot, "tmp")
	entries, err := os.ReadDir(tmpRoot)
	if err != nil && !os.IsNotExist(err) {
		log.Printf("chunk cleaner: read %s: %v", tmpRoot, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		live, err := h.cache.Exists(ctx, h.sessionKey(entry.Name()))
		if err != nil {
			// Redis 不可用时宁可不动：删掉活会话的分片等于毁掉用户的续传进度
			continue
		}
		if live {
			continue
		}
		if err := os.RemoveAll(filepath.Join(tmpRoot, entry.Name())); err != nil {
			log.Printf("chunk cleaner: remove upload %s: %v", entry.Name(), err)
		}
	}

	// 成品目录只清合并临时文件：.mp4 本身可能是已发布视频，不能凭磁盘状态删
	videoRoot := filepath.Join(uploadsRoot, "videos")
	_ = filepath.WalkDir(videoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".mp4.part") {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil || !info.ModTime().Before(cutoff) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			log.Printf("chunk cleaner: remove %s: %v", path, err)
		}
		return nil
	})
}
