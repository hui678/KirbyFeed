package account

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type AccountRepository struct {
	db *gorm.DB
}

func NewAccountRepository(db *gorm.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

func (ar *AccountRepository) CreateAccount(ctx context.Context, account *Account) error {
	if err := ar.db.WithContext(ctx).Create(account).Error; err != nil {
		return err
	}
	return nil
}

func (ar *AccountRepository) Rename(ctx context.Context, id uint, newUsername string) error {
	result := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("username", newUsername)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// RenameWithToken 在一个事务里改账号名、重签 token，并同步该账号历史视频上
// 冗余的 videos.username，返回受影响的视频 ID 供上层失效实体缓存。
//
// videos.username 是反范式冗余（Feed 列表靠它免 join 账号表），只改 accounts
// 会让旧视频永久显示旧名字 —— 重新发布才恢复，看起来像"改名没生效"。
func (ar *AccountRepository) RenameWithToken(ctx context.Context, id uint, newUsername string, token string) ([]uint, error) {
	var videoIDs []uint
	err := ar.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&Account{}).Where("id = ?", id).Updates(map[string]interface{}{
			"username": newUsername,
			"token":    token,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}

		if err := tx.Table("videos").Where("author_id = ?", id).Pluck("id", &videoIDs).Error; err != nil {
			return err
		}
		if len(videoIDs) > 0 {
			if err := tx.Table("videos").Where("author_id = ?", id).
				Update("username", newUsername).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return videoIDs, nil
}

func (ar *AccountRepository) ChangePassword(ctx context.Context, id uint, newPassword string) error {
	if err := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("password", newPassword).Error; err != nil {
		return err
	}
	return nil
}

func (ar *AccountRepository) FindByID(ctx context.Context, id uint) (*Account, error) {
	var account Account
	if err := ar.db.WithContext(ctx).First(&account, id).Error; err != nil {
		return nil, err
	}
	return &account, nil
}

func (ar *AccountRepository) FindByUsername(ctx context.Context, username string) (*Account, error) {
	var account Account
	if err := ar.db.WithContext(ctx).Where("username = ?", username).First(&account).Error; err != nil {
		return nil, err
	}
	return &account, nil
}

// Login 写入本轮签发的 access/refresh（均为哈希）与 refresh 的过期时间。
// 一张表一行 = 一个会话，覆盖写即「单会话互踢」。
func (ar *AccountRepository) Login(ctx context.Context, id uint, token, refreshTokenHash string, refreshExpiry time.Time) error {
	return ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Updates(map[string]interface{}{
		"token":                token,
		"refresh_token":        refreshTokenHash,
		"prev_refresh_token":   "",
		"refresh_rotated_at":   nil,
		"refresh_token_expiry": refreshExpiry,
	}).Error
}

// RotateRefresh 原子地把 refresh 从 oldHash 换成 newHash。
//
// 条件里带 refresh_token = oldHash（CAS）：两个并发刷新拿着同一张旧票过来，
// 只有一个能把行改掉，另一个 RowsAffected=0，从而走「幂等重放 / 拒绝」分支，
// 不会出现两个请求各自轮换出两张有效新票的情况。
func (ar *AccountRepository) RotateRefresh(ctx context.Context, id uint, oldHash, newHash string, rotatedAt time.Time) (bool, error) {
	result := ar.db.WithContext(ctx).Model(&Account{}).
		Where("id = ? AND refresh_token = ?", id, oldHash).
		Updates(map[string]interface{}{
			"refresh_token":      newHash,
			"prev_refresh_token": oldHash,
			"refresh_rotated_at": rotatedAt,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

func (ar *AccountRepository) Logout(ctx context.Context, id uint) error {
	if err := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Updates(map[string]interface{}{
		"token":              "",
		"refresh_token":      "",
		"prev_refresh_token": "",
		"refresh_rotated_at": nil,
	}).Error; err != nil {
		return err
	}
	return nil
}

func (ar *AccountRepository) UpdateAvatar(ctx context.Context, accountID uint, avatarURL string) error {
	return ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", accountID).Update("avatar_url", avatarURL).Error
}

func (ar *AccountRepository) UpdateToken(ctx context.Context, id uint, token string) error {
	return ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("token", token).Error
}

func (ar *AccountRepository) UpdateFields(ctx context.Context, id uint, updates map[string]interface{}) error {
	return ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Updates(updates).Error
}

// FindByRefreshToken 走 refresh_token / prev_refresh_token 上的二级索引单条命中。
//
// 必须同时匹配 prev：轮换后旧票还在宽限期内时仍要能被找到，
// 才能区分「弱网重试」（幂等重放）与「泄漏重用」（整账号吊销）。找不到就无法判定，只能一律拒绝。
//
// 修复要点：旧实现的降级路径是 FindAll() 全表扫描再内存里逐条比对，
// refresh 频率一高就是 DB 灾难（而且把全表密码哈希读进内存）。
func (ar *AccountRepository) FindByRefreshToken(ctx context.Context, refreshTokenHash string) (*Account, error) {
	if refreshTokenHash == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var account Account
	if err := ar.db.WithContext(ctx).
		Where("refresh_token = ? OR prev_refresh_token = ?", refreshTokenHash, refreshTokenHash).
		First(&account).Error; err != nil {
		return nil, err
	}
	return &account, nil
}
