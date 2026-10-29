package feed

import (
	"context"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"time"

	"gorm.io/gorm"
)

type FeedRepository struct {
	db *gorm.DB
}

func NewFeedRepository(db *gorm.DB) *FeedRepository {
	return &FeedRepository{db: db}
}

// ListLatest 按 (create_time DESC, id DESC) 取一页视频。
//
// 两处必须成对存在，缺一不可：
//  1. ORDER BY 里必须有 id 作为次级键 —— 否则相同 create_time 的行之间没有稳定顺序，
//     相邻两页的相对次序可能变化，造成重复或漏项。
//  2. WHERE 必须用复合比较而不是 create_time < ? —— 否则游标所在那一毫秒的其他视频会被整体跳过。
//
// InnoDB 二级索引尾部自动带上主键，idx_videos_create_time 实际是 (create_time, id)，
// 所以这组 ORDER BY + WHERE 可以完整走索引，不需要新建索引。
func (repo *FeedRepository) ListLatest(ctx context.Context, limit int, cursor *TimelineCursor) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("create_time DESC, id DESC")

	if cursor != nil {
		query = query.Where(
			"(create_time < ?) OR (create_time = ? AND id < ?)",
			cursor.CreateTime,
			cursor.CreateTime, cursor.ID,
		)
	}

	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

// ListByFollowing 与 ListLatest 共用同一套复合游标语义，理由见 ListLatest 注释。
func (repo *FeedRepository) ListByFollowing(ctx context.Context, limit int, viewerAccountID uint, cursor *TimelineCursor) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("create_time DESC, id DESC")
	if viewerAccountID > 0 {
		followingSubQuery := repo.db.WithContext(ctx).
			Model(&social.Social{}).
			Select("vlogger_id").
			Where("follower_id = ?", viewerAccountID)
		query = query.Where("author_id IN (?)", followingSubQuery)
	}
	if cursor != nil {
		query = query.Where(
			"(create_time < ?) OR (create_time = ? AND id < ?)",
			cursor.CreateTime,
			cursor.CreateTime, cursor.ID,
		)
	}
	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

func (repo *FeedRepository) ListByPopularity(ctx context.Context, limit int, popularityBefore int64, timeBefore time.Time, idBefore uint) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("popularity DESC, create_time DESC, id DESC")

	// 只有当游标完整提供时才加过滤（popularity 允许为 0）
	if !timeBefore.IsZero() && idBefore > 0 {
		query = query.Where(
			"(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)",
			popularityBefore,
			popularityBefore, timeBefore,
			popularityBefore, timeBefore, idBefore,
		)
	}

	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

func (repo *FeedRepository) GetByIDs(ctx context.Context, ids []uint) ([]*video.Video, error) {
	var videos []*video.Video
	if len(ids) == 0 {
		return videos, nil
	}
	if err := repo.db.WithContext(ctx).Model(&video.Video{}).
		Where("id IN ?", ids).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

// ListTags 返回视频数最多的标签，供话题流顶部标签栏使用。
// 只统计仍然存在的视频：videos 是硬删除，但 video_tags 不会级联删除，
// 不 JOIN videos 会把已删除视频的标签也统计进去。
func (repo *FeedRepository) ListTags(ctx context.Context, limit int) ([]TagSummary, error) {
	var tags []TagSummary
	err := repo.db.WithContext(ctx).
		Table("tags").
		Select("tags.name AS name, COUNT(video_tags.video_id) AS video_count").
		Joins("JOIN video_tags ON video_tags.tag_id = tags.id").
		Joins("JOIN videos ON videos.id = video_tags.video_id").
		Group("tags.id, tags.name").
		Order("video_count DESC, tags.name ASC").
		Limit(limit).
		Scan(&tags).Error
	return tags, err
}

// ListByTag 与 ListLatest 共用 (create_time DESC, id DESC) 的复合游标语义。
// Select("videos.*") 是必须的：JOIN 后 tags 表也有 id/name 列，不限定会覆盖视频字段。
func (repo *FeedRepository) ListByTag(ctx context.Context, tagName string, limit int, cursor *TimelineCursor) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).Table("videos").
		Select("videos.*").
		Joins("JOIN video_tags ON video_tags.video_id = videos.id").
		Joins("JOIN tags ON tags.id = video_tags.tag_id").
		Where("tags.name = ?", tagName).
		Order("videos.create_time DESC, videos.id DESC")

	if cursor != nil {
		query = query.Where(
			"(videos.create_time < ?) OR (videos.create_time = ? AND videos.id < ?)",
			cursor.CreateTime,
			cursor.CreateTime, cursor.ID,
		)
	}

	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}
