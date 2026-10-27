package account

import "time"

type Account struct {
	ID           uint   `gorm:"primaryKey" json:"id"`
	Username     string `gorm:"unique" json:"username"`
	Password     string `json:"-"`
	Token        string `json:"-"`
	RefreshToken string `gorm:"size:64;index" json:"-"`
	// RefreshTokenExpiry：refresh token 的绝对过期时间。
	// 修复要点：旧实现只在 Redis 侧用 TTL 表达「7 天」，MySQL 侧只有裸字符串，
	// 一旦 Redis 不可用或 key 被淘汰，refresh token 就变成永久有效。
	RefreshTokenExpiry time.Time `json:"-"`
	AvatarURL          string    `gorm:"type:varchar(512)" json:"avatar_url,omitempty"`
	Bio                string    `gorm:"type:varchar(255)" json:"bio,omitempty"`
}

type CreateAccountRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RenameRequest struct {
	NewUsername string `json:"new_username"`
}

type FindByIDRequest struct {
	ID uint `json:"id"`
}

type FindByIDResponse struct {
	ID        uint   `json:"id"`
	Username  string `json:"username"`
	AvatarURL string `json:"avatar_url,omitempty"`
	Bio       string `json:"bio,omitempty"`
}

type FindByUsernameRequest struct {
	Username string `json:"username"`
}

type FindByUsernameResponse struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

// ChangePasswordRequest：改密是登录态操作，账号身份来自 access token，
// 不再由请求体携带 username（否则知道用户名就能无限次试旧密码）。
type ChangePasswordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token        string `json:"token"`
	RefreshToken string `json:"refresh_token"`
	AccountID    uint   `json:"account_id"`
	Username     string `json:"username"`
}

type UpdateProfileRequest struct {
	AvatarURL string `json:"avatar_url"`
	Bio       string `json:"bio"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type GetProfileRequest struct {
	AccountID uint `json:"account_id"`
}

type GetProfileResponse struct {
	Account       FindByIDResponse `json:"account"`
	VideoCount    int64            `json:"video_count"`
	TotalLikes    int64            `json:"total_likes"`
	FollowerCount int64            `json:"follower_count"`
	VloggerCount  int64            `json:"vlogger_count"`
}
