package video

import (
	"context"
	"errors"
	"log"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
)

type LikeService struct {
	repo         *LikeRepository
	VideoRepo    *VideoRepository
	cache        *rediscache.Client
	likeMQ       *rabbitmq.LikeMQ
	popularityMQ *rabbitmq.PopularityMQ
}

func NewLikeService(repo *LikeRepository, videoRepo *VideoRepository, cache *rediscache.Client, likeMQ *rabbitmq.LikeMQ, popularityMQ *rabbitmq.PopularityMQ) *LikeService {
	return &LikeService{repo: repo, VideoRepo: videoRepo, cache: cache, likeMQ: likeMQ, popularityMQ: popularityMQ}
}

func (s *LikeService) Like(ctx context.Context, like *Like) error {
	if like == nil {
		return errors.New("like is nil")
	}
	if like.VideoID == 0 || like.AccountID == 0 {
		return errors.New("video_id and account_id are required")
	}

	if s.VideoRepo != nil {
		ok, err := s.VideoRepo.IsExist(ctx, like.VideoID)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("video not found")
		}
	}

	isLiked, err := s.repo.IsLiked(ctx, like.VideoID, like.AccountID)
	if err != nil {
		return err
	}
	if isLiked {
		return errors.New("user has liked this video")
	}

	// 落库事件：LikeWorker 消费，负责 likes 行 + videos.likes_count/popularity。
	// 热度事件：PopularityWorker 消费，只写 Redis 分钟桶。
	// 两条消息互不依赖：Redis 挂了照样能落库，MySQL 慢了也不会拖住热度。
	// 热度事件的幂等键：**发布前生成，MQ 与降级路径共用同一个**。
	//
	// 为什么不能用 PopularityMQ 内部生成的：存在「publish 成功但 confirm 超时」这个
	// 无法消除的窗口 —— 消息其实已经在队列里了，消费端迟早会处理一次；而服务层以为
	// 发布失败，又走降级直写一次。两条路径各加一遍，Redis 热度就翻倍了，
	// 而 MySQL 侧的 popularity 有 likes 表唯一索引保护只加一遍，两边直接对不上。
	popEventID, _ := rabbitmq.NewEventID()

	mysqlEnqueued := false
	redisEnqueued := false
	if s.likeMQ != nil {
		if err := s.likeMQ.Like(ctx, like.AccountID, like.VideoID); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, like.VideoID, 1, popEventID); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	// 降级：MQ 发布失败（连接断开 / broker nack / confirm 超时）时事务直写 MySQL，
	// 用户的这一次操作不能因为中间件挂了就丢掉。
	//
	// 这里刻意复用消费端同一个 ApplyLike：同一个事务、同一套 INSERT IGNORE 幂等。
	// 因为存在「publish 成功但 confirm 超时」这个无法消除的窗口 —— 消息其实已经在
	// 队列里了，worker 可能已经写完，此时降级再写一次必须是幂等的，否则计数会多加。
	if !mysqlEnqueued {
		if err := s.repo.ApplyLike(ctx, like.VideoID, like.AccountID); err != nil {
			return err
		}
	}

	// 降级：热度事件发布失败时直接更新 Redis（热度只是准实时数据，允许丢精度，
	// 但不允许因为 Redis 报错导致整个点赞失败，所以只记日志）。
	if !redisEnqueued {
		if err := UpdatePopularityCache(ctx, s.cache, like.VideoID, 1, popEventID); err != nil {
			log.Printf("like fallback: update popularity cache failed (video=%d): %v", like.VideoID, err)
		}
	}
	return nil
}

func (s *LikeService) Unlike(ctx context.Context, like *Like) error {
	if like == nil {
		return errors.New("like is nil")
	}
	if like.VideoID == 0 || like.AccountID == 0 {
		return errors.New("video_id and account_id are required")
	}

	if s.VideoRepo != nil {
		ok, err := s.VideoRepo.IsExist(ctx, like.VideoID)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("video not found")
		}
	}

	isLiked, err := s.repo.IsLiked(ctx, like.VideoID, like.AccountID)
	if err != nil {
		return err
	}
	if !isLiked {
		return errors.New("user has not liked this video")
	}

	// 同 Like：热度事件在发布前生成幂等键，MQ 与降级路径共用。
	popEventID, _ := rabbitmq.NewEventID()

	mysqlEnqueued := false
	redisEnqueued := false
	if s.likeMQ != nil {
		if err := s.likeMQ.Unlike(ctx, like.AccountID, like.VideoID); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, like.VideoID, -1, popEventID); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	// 降级：与 Like 对称，复用消费端同一个幂等事务入口。
	// RowsAffected=0（说明 worker 已经删过了）视为成功，不再报「未点赞」，
	// 因为用户的目标状态（未点赞）已经达成。
	if !mysqlEnqueued {
		if err := s.repo.ApplyUnlike(ctx, like.VideoID, like.AccountID); err != nil {
			return err
		}
	}

	if !redisEnqueued {
		if err := UpdatePopularityCache(ctx, s.cache, like.VideoID, -1, popEventID); err != nil {
			log.Printf("unlike fallback: update popularity cache failed (video=%d): %v", like.VideoID, err)
		}
	}
	return nil
}

func (s *LikeService) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	return s.repo.IsLiked(ctx, videoID, accountID)
}

func (s *LikeService) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	return s.repo.ListLikedVideos(ctx, accountID)
}
