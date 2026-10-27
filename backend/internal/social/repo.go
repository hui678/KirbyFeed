package social

import (
	"context"
	"feedsystem_video_go/internal/account"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SocialRepository struct {
	db *gorm.DB
}

func NewSocialRepository(db *gorm.DB) *SocialRepository {
	return &SocialRepository{db: db}
}

func (r *SocialRepository) Follow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).Create(social).Error
}

func (r *SocialRepository) Unfollow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
		Delete(&Social{}).Error
}

// ApplyFollow / ApplyUnfollow 是消费端专用的幂等写入入口。
//
// 与旧实现的区别：旧代码是「先 Create，捕获 1062 后直接 return nil」，
// 看起来幂等，实际上把「已存在」和「本次新建」混为一谈 —— 后面只要再加任何
// 副作用（粉丝数缓存、通知、计数），就无法判断该不该执行。
// 改成 INSERT IGNORE + RowsAffected 后，「这次到底有没有真的写入」是显式返回的，
// 后续副作用可以安全地挂在 created 分支里。
func (r *SocialRepository) ApplyFollow(ctx context.Context, social *Social) (created bool, err error) {
	if social == nil || social.FollowerID == 0 || social.VloggerID == 0 {
		return false, nil
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(social)
		if res.Error != nil {
			return res.Error
		}
		created = res.RowsAffected > 0
		// 预留：粉丝数 / 关注数如果有冗余计数列，应当在这里同事务更新，
		// 保证「关系行」与「计数」一起生效或一起回滚。
		return nil
	})
	if err != nil {
		return false, err
	}
	return created, nil
}

func (r *SocialRepository) ApplyUnfollow(ctx context.Context, social *Social) (deleted bool, err error) {
	if social == nil || social.FollowerID == 0 || social.VloggerID == 0 {
		return false, nil
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
			Delete(&Social{})
		if res.Error != nil {
			return res.Error
		}
		deleted = res.RowsAffected > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

func (r *SocialRepository) GetAllFollowers(ctx context.Context, VloggerID uint) ([]*account.Account, error) {
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("vlogger_id = ?", VloggerID).
		Limit(200).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	followerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		followerIDs = append(followerIDs, rel.FollowerID)
	}
	if len(followerIDs) == 0 {
		return []*account.Account{}, nil
	}

	var followers []*account.Account
	if err := r.db.WithContext(ctx).
		Model(&account.Account{}).
		Where("id IN ?", followerIDs).
		Find(&followers).Error; err != nil {
		return nil, err
	}
	return followers, nil
}

func (r *SocialRepository) GetAllVloggers(ctx context.Context, FollowerID uint) ([]*account.Account, error) {
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ?", FollowerID).
		Limit(200).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	vloggerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		vloggerIDs = append(vloggerIDs, rel.VloggerID)
	}
	if len(vloggerIDs) == 0 {
		return []*account.Account{}, nil
	}

	var vloggers []*account.Account
	if err := r.db.WithContext(ctx).
		Model(&account.Account{}).
		Where("id IN ?", vloggerIDs).
		Find(&vloggers).Error; err != nil {
		return nil, err
	}
	return vloggers, nil
}

func (r *SocialRepository) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *SocialRepository) CountFollowers(ctx context.Context, vloggerID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Social{}).Where("vlogger_id = ?", vloggerID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (r *SocialRepository) CountVloggers(ctx context.Context, followerID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Social{}).Where("follower_id = ?", followerID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}
