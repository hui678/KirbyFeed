package video

import (
	"context"
	"errors"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type CommentRepository struct {
	db *gorm.DB
}

func NewCommentRepository(db *gorm.DB) *CommentRepository {
	return &CommentRepository{db: db}
}

func (r *CommentRepository) CreateComment(ctx context.Context, comment *Comment) error {
	return r.db.WithContext(ctx).Create(comment).Error
}

func (r *CommentRepository) DeleteComment(ctx context.Context, comment *Comment) error {
	return r.db.WithContext(ctx).Delete(comment).Error
}

// ApplyPublish 是消费端唯一的评论写入入口：插评论 + 加热度同事务，靠 event_id 幂等。
//
// 与点赞同理：如果插行和热度更新是两条独立 SQL，插行成功、热度失败后重试会被
// event_id 唯一索引挡住，热度永远补不上。放进一个事务后重试才是幂等且完整的。
func (r *CommentRepository) ApplyPublish(ctx context.Context, comment *Comment) error {
	if comment == nil || comment.VideoID == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		exists, err := videoExistsTx(tx, comment.VideoID)
		if err != nil {
			return err
		}
		if !exists {
			// 视频已被删除，事件作废。
			return nil
		}

		res := tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(comment)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			// 同一 event_id 已经写过，重复投递，什么都不做。
			return nil
		}
		return bumpVideoColumn(tx, comment.VideoID, "popularity", 1)
	})
}

// ApplyDelete 删除评论，并在同一事务里把 popularity 减回去。
// 删除本身天然幂等：RowsAffected=0 说明已经删过了，此时绝不能再减热度。
func (r *CommentRepository) ApplyDelete(ctx context.Context, commentID uint) error {
	if commentID == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 先取 video_id：删除之后就查不到了。
		var comment Comment
		if err := tx.Select("video_id").First(&comment, commentID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}

		res := tx.Where("id = ?", commentID).Delete(&Comment{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			// 并发删除或已删除：热度不能减第二次。
			return nil
		}
		return bumpVideoColumn(tx, comment.VideoID, "popularity", -1)
	})
}

// FindByEventID 按幂等键查询，供兜底/排查使用。
func (r *CommentRepository) FindByEventID(ctx context.Context, eventID string) (*Comment, error) {
	if strings.TrimSpace(eventID) == "" {
		return nil, nil
	}
	var c Comment
	if err := r.db.WithContext(ctx).Where("event_id = ?", eventID).First(&c).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

func (r *CommentRepository) GetAllComments(ctx context.Context, videoID uint) ([]Comment, error) {
	var comments []Comment
	err := r.db.WithContext(ctx).
		Where("video_id = ?", videoID).
		Order("created_at asc").
		Limit(200).
		Find(&comments).Error
	return comments, err
}

func (r *CommentRepository) IsExist(ctx context.Context, id uint) (bool, error) {
	var comment Comment
	if err := r.db.WithContext(ctx).First(&comment, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *CommentRepository) GetByID(ctx context.Context, id uint) (*Comment, error) {
	var comment Comment
	if err := r.db.WithContext(ctx).First(&comment, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &comment, nil
}
