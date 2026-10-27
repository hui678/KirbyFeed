package account

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/auth"
	"log"
	"strconv"
	"strings"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type AccountService struct {
	accountRepository *AccountRepository
	cache             *rediscache.Client
}

var (
	ErrUsernameTaken       = errors.New("username already exists")
	ErrNewUsernameRequired = errors.New("new_username is required")
)

func NewAccountService(accountRepository *AccountRepository, cache *rediscache.Client) *AccountService {
	return &AccountService{accountRepository: accountRepository, cache: cache}
}

func (as *AccountService) CreateAccount(ctx context.Context, account *Account) error {
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(account.Password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	account.Password = string(passwordHash)
	if err := as.accountRepository.CreateAccount(ctx, account); err != nil {
		return err
	}
	return nil
}

func (as *AccountService) Rename(ctx context.Context, accountID uint, newUsername string) (string, error) {
	if newUsername == "" {
		return "", ErrNewUsernameRequired
	}

	token, err := auth.GenerateToken(accountID, newUsername)
	if err != nil {
		return "", err
	}

	if err := as.accountRepository.RenameWithToken(ctx, accountID, newUsername, token); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return "", ErrUsernameTaken
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", err
		}
		return "", err
	}
	// 改名会重签 token（JWT 里带着 username），旧的必须立刻失效
	as.setCurrentToken(ctx, accountID, token)
	return token, nil
}

// ChangePassword 改密。
//
// 修复要点：旧实现不要求登录，请求体里带 username 就能反复试旧密码（无鉴权 + 无限流 = 暴力破解入口）。
// 现在账号身份只能来自 access token（由 JWTAuth 中间件注入），并配套按账号限流。
func (as *AccountService) ChangePassword(ctx context.Context, accountID uint, oldPassword, newPassword string) error {
	if accountID == 0 {
		return errors.New("authentication required")
	}
	if oldPassword == "" || newPassword == "" {
		return errors.New("old_password and new_password are required")
	}
	account, err := as.FindByID(ctx, accountID)
	if err != nil {
		return err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(account.Password), []byte(oldPassword)); err != nil {
		return err
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if err := as.accountRepository.ChangePassword(ctx, account.ID, string(passwordHash)); err != nil {
		return err
	}
	// 改密即吊销：已签发的 access/refresh 全部作废，必须重新登录。
	return as.revokeTokens(ctx, account)
}

func (as *AccountService) FindByID(ctx context.Context, id uint) (*Account, error) {
	if account, err := as.accountRepository.FindByID(ctx, id); err != nil {
		return nil, err
	} else {
		return account, nil
	}
}

func (as *AccountService) FindByUsername(ctx context.Context, username string) (*Account, error) {
	if account, err := as.accountRepository.FindByUsername(ctx, username); err != nil {
		return nil, err
	} else {
		return account, nil
	}
}

func (as *AccountService) Login(ctx context.Context, username, password string) (string, string, error) {
	account, err := as.FindByUsername(ctx, username)
	if err != nil {
		return "", "", err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(account.Password), []byte(password)); err != nil {
		return "", "", err
	}
	accessToken, err := auth.GenerateToken(account.ID, account.Username)
	if err != nil {
		return "", "", err
	}
	refreshToken, err := auth.GenerateRefreshToken(account.ID)
	if err != nil {
		return "", "", err
	}
	refreshExpiry := time.Now().Add(auth.RefreshTokenTTL)
	if err := as.accountRepository.Login(ctx, account.ID, accessToken, refreshToken, refreshExpiry); err != nil {
		return "", "", err
	}
	if as.cache != nil {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		// 单会话互踢：上一张 refresh 的反查键必须清掉，不能只靠 TTL 等它过期
		if account.RefreshToken != "" && account.RefreshToken != refreshToken {
			as.mustClear(ctx, as.cache.Key("refresh:%s", account.RefreshToken))
		}
		// 当前生效 token 换成新的；写不进去就把快路径删掉，绝不留旧值。
		// 这一步做不干净，旧设备就踢不掉（旧值命中 == 旧 token 被判活放行）。
		as.setCurrentToken(ctx, account.ID, accessToken)

		if err := as.cache.SetBytes(cacheCtx, as.cache.Key("account:%d:refresh", account.ID), []byte(refreshToken), auth.RefreshTokenTTL); err != nil {
			log.Printf("failed to set refresh cache: %v", err)
		}
		if err := as.cache.SetBytes(cacheCtx, as.cache.Key("refresh:%s", refreshToken), []byte(strconv.FormatUint(uint64(account.ID), 10)), auth.RefreshTokenTTL); err != nil {
			log.Printf("failed to set refresh lookup: %v", err)
		}
	}
	return accessToken, refreshToken, nil
}

func (as *AccountService) Logout(ctx context.Context, accountID uint) error {
	account, err := as.FindByID(ctx, accountID)
	if err != nil {
		return err
	}
	if account.Token == "" && account.RefreshToken == "" {
		// MySQL 已空（重复登出），但 Redis 里可能还残留着上一次清理失败留下的脏值，
		// 它同样会让旧 token 被判活放行。这里必须再清一次才能提前返回。
		as.mustClear(ctx, as.accessTokenCacheKey(accountID))
		return nil
	}
	return as.revokeTokens(ctx, account)
}

// cacheTimeout：Redis 操作的统一短超时。宁可让快路径失效回源 MySQL，也不让鉴权被 Redis 拖住。
const cacheTimeout = 50 * time.Millisecond

func (as *AccountService) accessTokenCacheKey(accountID uint) string {
	return as.cache.Key("account:%d", accountID)
}

// setCurrentToken 把「当前生效的 access token」写进 Redis 快路径。
//
// 关键点：写失败时必须回退到删除，而不是「下次再说」。
// 判活规则是「Redis 里的值 == 传入 token」才算通过（见 middleware/jwt 的 isActive），
// 如果新 token 没写进去、旧值还在，那张**已经被顶掉的旧 token 仍会被判活放行** —— 互踢就此失效。
// 所以这里只有两个可接受的结果：写进新值，或者把 key 清掉让后续回源 MySQL。
func (as *AccountService) setCurrentToken(ctx context.Context, accountID uint, token string) {
	if as.cache == nil {
		return
	}
	key := as.accessTokenCacheKey(accountID)

	cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()

	// TTL 不超过 token 自身的剩余寿命：万一再出现 Redis 与 MySQL 不一致，
	// 残留窗口的上限就是 15 分钟（一个 access token 的生命周期），而不是原来的 24 小时。
	if err := as.cache.SetBytes(cacheCtx, key, []byte(token), auth.AccessTokenTTL); err != nil {
		log.Printf("failed to set access token cache: %v; dropping stale fast-path", err)
		as.mustClear(ctx, key)
	}
}

// mustClear 保证 Redis 侧不再残留「可以用来放行的旧值」。
//
// 三级降级：
//  1. DEL —— 语义最干净，删掉后判活自然回源 MySQL；
//  2. 重试一次 DEL —— 覆盖掉偶发的网络抖动；
//  3. 写入空哨兵 —— 空串不可能等于任何真实 token，判活必然不相等从而穿透回源复核。
//
// 三级都失败说明 Redis 整体不可用，而那时 GET 同样会失败、一样会回源 MySQL，
// 所以「Redis 残旧值 + 判定放行」这个组合在这里被彻底堵死。
func (as *AccountService) mustClear(ctx context.Context, key string) {
	if as.cache == nil {
		return
	}
	var err error
	for i := 0; i < 2; i++ {
		c, cancel := context.WithTimeout(ctx, cacheTimeout)
		err = as.cache.Del(c, key)
		cancel()
		if err == nil {
			return
		}
	}

	log.Printf("ERROR: failed to del cache key %s after retry: %v; writing empty sentinel", key, err)
	c, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()
	if err := as.cache.SetBytes(c, key, []byte{}, auth.AccessTokenTTL); err != nil {
		log.Printf("ERROR: failed to write empty sentinel for %s: %v", key, err)
	}
}

// revokeTokens 一次性作废该账号的 access / refresh：先让 Redis 失效，再清 MySQL（权威）。
//
// 两个方向都要做：只删 Redis，Redis 故障期间的请求会回源到还没清的 DB；
// 只清 MySQL，Redis 里残留的旧 token 仍会被快路径放行（这正是 mustClear 要解决的问题）。
// 顺序也不能反：先清 MySQL 会留下「MySQL 已吊销、Redis 仍放行」的窗口。
func (as *AccountService) revokeTokens(ctx context.Context, account *Account) error {
	as.mustClear(ctx, as.accessTokenCacheKey(account.ID))
	as.mustClear(ctx, as.cache.Key("account:%d:refresh", account.ID))
	if account.RefreshToken != "" {
		as.mustClear(ctx, as.cache.Key("refresh:%s", account.RefreshToken))
	}
	return as.accountRepository.Logout(ctx, account.ID)
}

func (as *AccountService) UpdateAvatar(ctx context.Context, accountID uint, avatarURL string) error {
	return as.accountRepository.UpdateAvatar(ctx, accountID, avatarURL)
}

func (as *AccountService) UpdateProfile(ctx context.Context, accountID uint, req *UpdateProfileRequest) error {
	updates := map[string]interface{}{}
	if req.Bio != "" {
		updates["bio"] = strings.TrimSpace(req.Bio)
	}
	if req.AvatarURL != "" {
		updates["avatar_url"] = strings.TrimSpace(req.AvatarURL)
	}
	if len(updates) == 0 {
		return errors.New("nothing to update")
	}
	return as.accountRepository.UpdateFields(ctx, accountID, updates)
}

// RefreshAccessToken 用 refresh token 换一张新的 access token。
//
// 链路：
//  1. 主路径走 Redis 反查键 refresh:<token> -> accountID（O(1)，且天然带 7 天 TTL）；
//  2. Redis 未命中 / 不可用时降级 MySQL，走 refresh_token 索引等值查询（旧实现是全表扫描）；
//  3. 无论走哪条路，最终都以 MySQL 里存的 refresh_token 为准做比对并校验过期时间；
//  4. 成功后回写 account:<id>，让后续鉴权继续走 Redis 快路径。
//
// 已知取舍：不轮换 refresh token（换 access 时原样返回旧 refresh），换取刷新链路简单、
// 前端无需处理并发刷新的 token 竞态；代价是 refresh 泄漏后在其剩余生命周期内都可用。
func (as *AccountService) RefreshAccessToken(ctx context.Context, refreshToken string) (string, uint, string, error) {
	if refreshToken == "" {
		return "", 0, "", errors.New("refresh token is empty")
	}

	// 1) Redis 主路径：反查键直接给出 accountID
	var account *Account
	if as.cache != nil {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		b, err := as.cache.GetBytes(cacheCtx, as.cache.Key("refresh:%s", refreshToken))
		cancel()
		if err == nil {
			if id, parseErr := strconv.ParseUint(string(b), 10, 64); parseErr == nil && id != 0 {
				if acc, dbErr := as.FindByID(ctx, uint(id)); dbErr == nil {
					account = acc
				}
			}
		}
	}

	// 2) Redis 不可用/未命中：降级 MySQL 索引查询
	if account == nil {
		acc, err := as.accountRepository.FindByRefreshToken(ctx, refreshToken)
		if err != nil {
			return "", 0, "", errors.New("invalid refresh token")
		}
		account = acc
	}

	// 3) 以 MySQL 为权威做比对 + 校验过期时间
	//    （Redis 只是加速，不是信任源：登出/改密/互踢都要靠这一步兜底）
	if account.RefreshToken == "" || account.RefreshToken != refreshToken {
		return "", 0, "", errors.New("invalid refresh token")
	}
	if account.RefreshTokenExpiry.IsZero() || time.Now().After(account.RefreshTokenExpiry) {
		return "", 0, "", errors.New("refresh token expired")
	}

	newToken, err := auth.GenerateToken(account.ID, account.Username)
	if err != nil {
		return "", 0, "", err
	}
	if err := as.accountRepository.UpdateToken(ctx, account.ID, newToken); err != nil {
		return "", 0, "", err
	}

	// 4) 回写快路径缓存（旧实现的 MySQL 降级分支漏了这一步，
	//    导致降级成功后每个请求都要回源 DB 校验 access token）
	as.setCurrentToken(ctx, account.ID, newToken)

	return newToken, account.ID, account.Username, nil
}
