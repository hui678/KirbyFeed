package video

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"feedsystem_video_go/internal/apierror"
	"feedsystem_video_go/internal/dbguard"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"gorm.io/gorm"
)

type VideoService struct {
	repo         *VideoRepository
	cache        *rediscache.Client
	cacheTTL     time.Duration
	popularityMQ *rabbitmq.PopularityMQ
}

func NewVideoService(repo *VideoRepository, cache *rediscache.Client, popularityMQ *rabbitmq.PopularityMQ) *VideoService {
	return &VideoService{repo: repo, cache: cache, cacheTTL: VideoEntityTTL, popularityMQ: popularityMQ}
}

func (vs *VideoService) Publish(ctx context.Context, video *Video) error {
	if video == nil {
		return errors.New("video is nil")
	}
	video.Title = strings.TrimSpace(video.Title)
	video.PlayURL = strings.TrimSpace(video.PlayURL)
	video.CoverURL = strings.TrimSpace(video.CoverURL)

	if video.Title == "" {
		return errors.New("title is required")
	}
	if video.PlayURL == "" {
		return errors.New("play url is required")
	}
	if video.CoverURL == "" {
		return errors.New("cover url is required")
	}

	//事务保证视频写入库和消息写入本地消息表的一致性
	err := vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(video).Error; err != nil {
			return err
		}

		// EventID 在这里、也就是写入 outbox 的同一个事务里生成并落库。
		// 之后无论 poller 重投多少次，带的都是同一个 event_id，
		// 消费端才能靠它去重（confirm 丢失 ⇒ 必然重投，这是 Outbox 无法消除的前提）。
		eventID, err := rabbitmq.NewEventID()
		if err != nil {
			return err
		}
		msg := OutboxMsg{
			VideoID:    video.ID,
			EventType:  "video_published",
			Status:     OutboxStatusPending,
			CreateTime: video.CreateTime,
			EventID:    &eventID,
		}

		if err := tx.Create(&msg).Error; err != nil {
			return err
		}

		tags := ExtractTags(video.Title + " " + video.Description)
		for _, tagName := range tags {
			var tag Tag
			if err := tx.Where("name = ?", tagName).FirstOrCreate(&tag, Tag{Name: tagName}).Error; err != nil {
				return err
			}
			if err := tx.Create(&VideoTag{VideoID: video.ID, TagID: tag.ID}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err

}

func (vs *VideoService) Delete(ctx context.Context, id uint, authorID uint) error {
	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if video == nil {
		return errors.New("video not found")
	}
	if video.AuthorID != authorID {
		return apierror.ErrUnauthorized
	}
	if err := vs.repo.DeleteVideo(ctx, id); err != nil {
		return err
	}
	// 与 Feed 批量读取共用同一份实体缓存
	InvalidateVideoEntity(context.Background(), vs.cache, id)

	// 从热时间线移除，避免已删除视频继续占用当页名额。
	// Redis 失败不回滚删除：源数据已经没了，读路径装配时也会把它过滤掉，
	// 这里只是让页更满；残留项由后台对账与空 ZSET 重建兜底。
	if vs.cache != nil {
		opCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := vs.cache.ZRem(opCtx, TimelineZSetKey(vs.cache), strconv.FormatUint(uint64(id), 10)); err != nil {
			log.Printf("删除视频后移除热时间线失败: video=%d err=%v", id, err)
		}
		cancel()
	}
	return nil
}

func (vs *VideoService) ListByAuthorID(ctx context.Context, authorID uint) ([]Video, error) {
	videos, err := vs.repo.ListByAuthorID(ctx, int64(authorID))
	if err != nil {
		return nil, err
	}
	return videos, nil
}

func (vs *VideoService) GetDetail(ctx context.Context, id uint) (*Video, error) {
	cacheKey := VideoEntityKey(vs.cache, id)

	// 命中返回 (*Video, true)；缓存明确记录「不存在」时返回 (nil, true)。
	getCached := func() (*Video, bool) {
		opCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		defer cancel()

		b, err := vs.cache.GetBytes(opCtx, cacheKey)
		if err != nil {
			return nil, false
		}
		if IsVideoEntityNull(string(b)) { // 占位值：确定不存在，不再回源
			return nil, true
		}
		var cached Video
		if err := json.Unmarshal(b, &cached); err != nil {
			return nil, false
		}
		return &cached, true
	}

	setCached := func(video *Video, version string) {
		b, err := json.Marshal(video)
		if err != nil {
			return
		}
		// 版本 CAS：回源期间如果发生过失效，这次回写就携带了旧值，必须丢弃。
		SetVideoEntityIfVersion(ctx, vs.cache, video.ID, b, vs.cacheTTL, version)
	}

	if vs.cache != nil {
		if v, ok := getCached(); ok {
			if v == nil { // 缓存明确记录了「不存在」
				return nil, gorm.ErrRecordNotFound
			}
			return v, nil
		}

		// 回源用进程内 singleflight 去重，与 Feed 批量读取共用同一个 Group。
		// 主键单行查询成本极小而并发量极大，用分布式锁不划算（理由见 EntityLoadGroup 的注释）。
		v, err, _ := EntityLoadGroup.Do(EntitySingleflightKey(id), func() (interface{}, error) {
			if v, ok := getCached(); ok { // 双重检查：等待期间可能已被回填
				if v == nil {
					return (*Video)(nil), gorm.ErrRecordNotFound
				}
				return v, nil
			}

			// 版本号必须在查库之前读取：失效可能发生在「查库之后、回写之前」，
			// 那种情况下查回的是旧值，CAS 必须失败。读不到版本（Redis 故障）时
			// 只查库、不回写，因为比较基准不可信。
			version, versionOK := EntityVersion(ctx, vs.cache, id)

			var video *Video
			if err := dbguard.Do(func() error {
				var e error
				video, e = vs.repo.GetByID(ctx, id)
				return e
			}); err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// 记下「这个 ID 确定不存在」，否则同一个不存在的 ID 每次请求
					// 都会打一次 MySQL（缓存穿透）。短 TTL，数据补上后自动恢复。
					SetVideoEntityNull(ctx, vs.cache, id)
				}
				return (*Video)(nil), err
			}
			if versionOK {
				setCached(video, version)
			}
			return video, nil
		})
		if err != nil {
			return nil, err
		}

		// singleflight 把同一个指针返回给所有等待者，而等待者是不同用户的请求。
		// 各自持有副本，避免下游填充个性化字段时污染其他请求的响应。
		safeCopy := *(v.(*Video))
		return &safeCopy, nil
	}

	var video *Video
	if err := dbguard.Do(func() error {
		var e error
		video, e = vs.repo.GetByID(ctx, id)
		return e
	}); err != nil {
		return nil, err
	}
	return video, nil
}

func (vs *VideoService) UpdateLikesCount(ctx context.Context, id uint, likesCount int64) error {
	if err := vs.repo.UpdateLikesCount(ctx, id, likesCount); err != nil {
		return err
	}
	return nil
}

func (vs *VideoService) UpdatePopularity(ctx context.Context, id uint, change int64) error {
	if err := vs.repo.UpdatePopularity(ctx, id, change); err != nil {
		return err
	}

	// 热度事件同样在发布前生成幂等键，MQ 与降级路径共用。
	popEventID, _ := rabbitmq.NewEventID()

	if vs.popularityMQ != nil {
		if err := vs.popularityMQ.Update(ctx, id, change, popEventID); err == nil {
			return nil
		}
	}

	// 热度缓存的更新（失效实体缓存 + 写分钟窗 ZSET）在 UpdatePopularityCache 里已经有一份
	// 实现，这里直接复用：原先那份内联副本把 50ms 的 context 同时给了 ZincrBy 和 Expire，
	// 属于「带 timeout 的 context 跨 IO 步骤复用」，两份实现必然有一处漏修。
	// 这里只记日志不返回错误：DB 已经写成功了，热度缓存失败不应该让整个接口报错。
	if err := UpdatePopularityCache(ctx, vs.cache, id, change, popEventID); err != nil {
		log.Printf("update popularity cache failed (video=%d): %v", id, err)
	}
	return nil
}
