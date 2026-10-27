package video

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/apierror"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"log"
	"regexp"
	"strings"
)

type CommentService struct {
	repo            *CommentRepository
	VideoRepository *VideoRepository
	cache           *rediscache.Client
	commentMQ       *rabbitmq.CommentMQ
	popularityMQ    *rabbitmq.PopularityMQ
}

func NewCommentService(repo *CommentRepository, videoRepo *VideoRepository, cache *rediscache.Client, commentMQ *rabbitmq.CommentMQ, popularityMQ *rabbitmq.PopularityMQ) *CommentService {
	return &CommentService{repo: repo, VideoRepository: videoRepo, cache: cache, commentMQ: commentMQ, popularityMQ: popularityMQ}
}

func (s *CommentService) Publish(ctx context.Context, comment *Comment) error {
	if comment == nil {
		return errors.New("comment is nil")
	}
	comment.Username = strings.TrimSpace(comment.Username)
	comment.Content = strings.TrimSpace(comment.Content)
	if comment.VideoID == 0 || comment.AuthorID == 0 {
		return errors.New("video_id and author_id are required")
	}
	if comment.Content == "" {
		return errors.New("content is required")
	}

	exists, err := s.VideoRepository.IsExist(ctx, comment.VideoID)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("video not found")
	}

	// 幂等键在发布前就生成：无论最终走 MQ 还是降级直写，都写进 comments.event_id。
	// 这样「publish 成功但 confirm 超时 → 降级又写一次」这个窗口下，
	// 消费端的唯一索引能把第二次插入挡掉，不会出现两条一模一样的评论。
	if comment.EventID == nil {
		if eventID, err := rabbitmq.NewEventID(); err == nil && eventID != "" {
			comment.EventID = &eventID
		}
	}

	mysqlEnqueued := false
	redisEnqueued := false
	if s.commentMQ != nil {
		eventID := ""
		if comment.EventID != nil {
			eventID = *comment.EventID
		}
		if err := s.commentMQ.Publish(ctx, comment.Username, comment.VideoID, comment.AuthorID, comment.Content, eventID); err == nil {
			mysqlEnqueued = true
		}
	}
	// 热度事件单独生成幂等键：评论的 event_id 是给 comments 表唯一索引用的，
	// 这是另一条消息、另一条链路，两者不该共用一个键（否则将来给评论消费端加
	// 去重时会互相干扰）。理由同 Like：MQ 与降级路径必须共用同一个 ID。
	popEventID, _ := rabbitmq.NewEventID()

	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, comment.VideoID, 1, popEventID); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		s.notifyMentions(ctx, comment)
		return nil
	}

	// 降级：MQ 发布失败时复用消费端同一个幂等事务入口（插评论 + 加热度同事务）。
	if !mysqlEnqueued {
		if err := s.repo.ApplyPublish(ctx, comment); err != nil {
			return err
		}
	}

	// 降级：热度事件发布失败时直写 Redis；热度是准实时数据，失败只记日志。
	if !redisEnqueued {
		if err := UpdatePopularityCache(ctx, s.cache, comment.VideoID, 1, popEventID); err != nil {
			log.Printf("comment fallback: update popularity cache failed (video=%d): %v", comment.VideoID, err)
		}
	}
	s.notifyMentions(ctx, comment)
	return nil
}

func (s *CommentService) Delete(ctx context.Context, commentID uint, accountID uint) error {
	comment, err := s.repo.GetByID(ctx, commentID)
	if err != nil {
		return err
	}
	if comment == nil {
		return errors.New("comment not found")
	}
	if comment.AuthorID != accountID {
		return apierror.ErrUnauthorized
	}
	if s.commentMQ != nil {
		if err := s.commentMQ.Delete(ctx, commentID); err == nil {
			return nil
		}
	}
	// 降级：直接删除。删除天然幂等，重复执行不会报错也不会产生副作用。
	return s.repo.ApplyDelete(ctx, commentID)
}

func (s *CommentService) GetAll(ctx context.Context, videoID uint) ([]Comment, error) {
	exists, err := s.VideoRepository.IsExist(ctx, videoID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("video not found")
	}
	return s.repo.GetAllComments(ctx, videoID)
}

var mentionRegex = regexp.MustCompile(`@(\w+)`)

func (s *CommentService) notifyMentions(ctx context.Context, comment *Comment) {
	matches := mentionRegex.FindAllStringSubmatch(comment.Content, -1)
	if len(matches) == 0 {
		return
	}
	seen := make(map[string]bool)
	for _, m := range matches {
		username := m[1]
		if seen[username] || username == comment.Username {
			continue
		}
		seen[username] = true
		var accID uint
		if err := s.repo.db.WithContext(ctx).Table("accounts").Where("username = ?", username).Select("id").Scan(&accID).Error; err != nil || accID == 0 {
			continue
		}
		notif := struct {
			RecipientID uint
			SenderID    uint
			Type        string
			TargetID    uint
			Content     string
		}{
			RecipientID: accID,
			SenderID:    comment.AuthorID,
			Type:        "mention",
			TargetID:    comment.VideoID,
			Content:     comment.Username + " 在评论中提到了你",
		}
		if err := s.repo.db.WithContext(ctx).Table("notifications").Create(&notif).Error; err != nil {
			log.Printf("create mention notification failed: %v", err)
		}
	}
}
