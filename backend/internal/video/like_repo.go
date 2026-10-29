package video

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ApplyLike 是消费端唯一的点赞写入入口：事件去重 + 插点赞行 + 累加计数放在同一个事务里。
//
// 为什么必须是事务：插行成功但计数失败的旧实现里，重试会命中唯一索引直接跳过，
// 计数就再也补不回来 —— 一个「最终一致」的系统里最不该出现的就是这种永久偏差。
// 事务让「事件已处理」「行存在」「计数已加」要么同时成立、要么都不成立，重试才是安全的。
//
// 幂等两层：
//  1. event_id 台账（like_events）挡住同一条事件的重投，包括「like 处理完、用户已取消、
//     旧 like 被重投」这种唯一索引已经挡不住的复活；
//  2. 唯一索引 idx_like_video_account + INSERT IGNORE 挡住不同事件但同一业务状态的重复。
func (r *LikeRepository) ApplyLike(ctx context.Context, videoID, accountID uint, eventID string) error {
	if videoID == 0 || accountID == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		exists, err := videoExistsTx(tx, videoID)
		if err != nil {
			return err
		}
		if !exists {
			// 视频已被删除：事件作废，Ack 掉即可，不算失败。
			return nil
		}

		fresh, err := markLikeEvent(tx, eventID, videoID, accountID, "like")
		if err != nil {
			return err
		}
		if !fresh {
			return nil // 同一条事件重投，已经生效过
		}

		res := tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&Like{
			VideoID:   videoID,
			AccountID: accountID,
			CreatedAt: time.Now(),
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil
		}

		if err := bumpVideoColumn(tx, videoID, "likes_count", 1); err != nil {
			return err
		}
		return bumpVideoColumn(tx, videoID, "popularity", 1)
	})
}

// ApplyUnlike 与 ApplyLike 对称：事件去重 + 删除行 + 减计数同事务。
// 删除的 RowsAffected=0 表示本来就没点赞（或已取消），不重复减计数。
func (r *LikeRepository) ApplyUnlike(ctx context.Context, videoID, accountID uint, eventID string) error {
	if videoID == 0 || accountID == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		fresh, err := markLikeEvent(tx, eventID, videoID, accountID, "unlike")
		if err != nil {
			return err
		}
		if !fresh {
			return nil
		}

		res := tx.Where("video_id = ? AND account_id = ?", videoID, accountID).Delete(&Like{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil
		}
		if err := bumpVideoColumn(tx, videoID, "likes_count", -1); err != nil {
			return err
		}
		return bumpVideoColumn(tx, videoID, "popularity", -1)
	})
}

// markLikeEvent 在事务里占住 event_id。返回 false 表示这条事件已经处理过。
// eventID 为空时退化为不去重（兼容历史消息），但新消息都会带 id。
func markLikeEvent(tx *gorm.DB, eventID string, videoID, accountID uint, action string) (bool, error) {
	if eventID == "" {
		return true, nil
	}
	res := tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&LikeEventLog{
		EventID:   eventID,
		VideoID:   videoID,
		AccountID: accountID,
		Action:    action,
		CreatedAt: time.Now(),
	})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, nil
}

// PurgeLikeEvents 清理过期的去重台账，避免表随点赞量无限增长。
func (r *LikeRepository) PurgeLikeEvents(ctx context.Context, before time.Time) (int64, error) {
	res := r.db.WithContext(ctx).Where("created_at < ?", before).Delete(&LikeEventLog{})
	return res.RowsAffected, res.Error
}

// bumpVideoColumn 原子自增/自减，并用 GREATEST 兜底不让计数变负。
// 列名走白名单，避免把外部输入拼进 SQL。
func bumpVideoColumn(tx *gorm.DB, videoID uint, column string, change int64) error {
	switch column {
	case "likes_count", "popularity", "comments_count":
	default:
		return fmt.Errorf("unsupported video column: %s", column)
	}
	return tx.Model(&Video{}).
		Where("id = ?", videoID).
		UpdateColumn(column, gorm.Expr("GREATEST("+column+" + ?, 0)", change)).Error
}

func videoExistsTx(tx *gorm.DB, videoID uint) (bool, error) {
	var n int64
	if err := tx.Model(&Video{}).Where("id = ?", videoID).Count(&n).Error; err != nil {
		return false, err
	}
	return n > 0, nil
}

type LikeRepository struct {
	db *gorm.DB
}

func NewLikeRepository(db *gorm.DB) *LikeRepository {
	return &LikeRepository{db: db}
}

func (r *LikeRepository) Like(ctx context.Context, like *Like) error {
	return r.db.WithContext(ctx).Create(like).Error
}

func (r *LikeRepository) Unlike(ctx context.Context, like *Like) error {
	return r.db.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", like.VideoID, like.AccountID).
		Delete(&Like{}).Error
}

func (r *LikeRepository) LikeIgnoreDuplicate(ctx context.Context, like *Like) (created bool, err error) {
	if like == nil || like.VideoID == 0 || like.AccountID == 0 {
		return false, nil
	}
	err = r.db.WithContext(ctx).Create(like).Error
	if err == nil {
		return true, nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return false, nil
	}
	return false, err
}

func (r *LikeRepository) DeleteByVideoAndAccount(ctx context.Context, videoID, accountID uint) (deleted bool, err error) {
	if videoID == 0 || accountID == 0 {
		return false, nil
	}
	res := r.db.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Delete(&Like{})
	return res.RowsAffected > 0, res.Error
}

func (r *LikeRepository) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&Like{}).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *LikeRepository) BatchGetLiked(ctx context.Context, videoIDs []uint, accountID uint) (map[uint]bool, error) {
	likeMap := make(map[uint]bool)
	if len(videoIDs) == 0 {
		return likeMap, nil
	}
	if accountID == 0 {
		return likeMap, nil
	}
	var likes []Like
	err := r.db.WithContext(ctx).Model(&Like{}).
		Where("video_id IN ? AND account_id = ?", videoIDs, accountID).
		Find(&likes).Error
	if err != nil {
		return nil, err
	}
	for _, like := range likes {
		likeMap[like.VideoID] = true
	}
	return likeMap, nil
}

func (r *LikeRepository) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	var videos []Video
	if accountID == 0 {
		return videos, nil
	}
	err := r.db.WithContext(ctx).
		Model(&Video{}).
		Joins("JOIN likes ON likes.video_id = videos.id").
		Where("likes.account_id = ?", accountID).
		Order("likes.created_at desc").
		Limit(200).
		Find(&videos).Error
	if err != nil {
		return nil, err
	}
	return videos, nil
}
