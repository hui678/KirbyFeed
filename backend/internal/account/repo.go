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

func (ar *AccountRepository) RenameWithToken(ctx context.Context, id uint, newUsername string, token string) error {
	return ar.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&Account{}).Where("id = ?", id).Update("username", newUsername)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		if err := tx.Model(&Account{}).Where("id = ?", id).Update("token", token).Error; err != nil {
			return err
		}
		return nil
	})
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

// Login 写入本轮签发的 access/refresh token 与 refresh 的过期时间。
// 一张表一行 = 一个会话，覆盖写即「单会话互踢」。
func (ar *AccountRepository) Login(ctx context.Context, id uint, token, refreshToken string, refreshExpiry time.Time) error {
	return ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Updates(map[string]interface{}{
		"token":                token,
		"refresh_token":        refreshToken,
		"refresh_token_expiry": refreshExpiry,
	}).Error
}

func (ar *AccountRepository) Logout(ctx context.Context, id uint) error {
	if err := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Updates(map[string]interface{}{"token": "", "refresh_token": ""}).Error; err != nil {
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

// FindByRefreshToken 走 refresh_token 上的二级索引单条命中。
//
// 修复要点：刷新接口 Redis 未命中时的降级路径原来是 FindAll() 全表扫描再内存里逐条比对，
// refresh 频率一高就是 DB 灾难（而且把全表密码哈希读进内存）。这里改成索引等值查询。
func (ar *AccountRepository) FindByRefreshToken(ctx context.Context, refreshToken string) (*Account, error) {
	if refreshToken == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var account Account
	if err := ar.db.WithContext(ctx).Where("refresh_token = ?", refreshToken).First(&account).Error; err != nil {
		return nil, err
	}
	return &account, nil
}
