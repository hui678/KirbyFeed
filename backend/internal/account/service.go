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
	"feedsystem_video_go/internal/videocache"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

// accountStore 收窄业务层对持久层的依赖：业务规则只依赖这些方法，
// 这样刷新轮换、重用检测、吊销这些关键路径可以脱离 MySQL 做单元测试。
type accountStore interface {
	CreateAccount(ctx context.Context, account *Account) error
	FindByID(ctx context.Context, id uint) (*Account, error)
	FindByUsername(ctx context.Context, username string) (*Account, error)
	Login(ctx context.Context, id uint, token, refreshTokenHash string, refreshExpiry time.Time) error
	Logout(ctx context.Context, id uint) error
	ChangePassword(ctx context.Context, id uint, newPassword string) error
	RenameWithToken(ctx context.Context, id uint, newUsername string, token string) ([]uint, error)
	UpdateAvatar(ctx context.Context, accountID uint, avatarURL string) error
	UpdateFields(ctx context.Context, id uint, updates map[string]interface{}) error
	UpdateToken(ctx context.Context, id uint, token string) error
	FindByRefreshToken(ctx context.Context, refreshTokenHash string) (*Account, error)
	RotateRefresh(ctx context.Context, id uint, oldHash, newHash string, rotatedAt time.Time) (bool, error)
}

type AccountService struct {
	accountRepository accountStore
	cache             *rediscache.Client
}

var (
	ErrUsernameTaken       = errors.New("username already exists")
	ErrNewUsernameRequired = errors.New("new_username is required")
	ErrRefreshInvalid      = errors.New("invalid refresh token")
	ErrRefreshExpired      = errors.New("refresh token expired")
	ErrRefreshReuse        = errors.New("refresh token reuse detected")
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

	videoIDs, err := as.accountRepository.RenameWithToken(ctx, accountID, newUsername, token)
	if err != nil {
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

	// 事务已提交，再失效实体缓存。缓存里存的是含 username 的整行 JSON，
	// 不失效的话详情页 / Feed 会继续展示旧名字，直到 TTL 到期。
	// 失效失败只记日志：DB 已是权威值，缓存最迟一个 EntityTTL 后自然收敛。
	for _, id := range videoIDs {
		cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
		err := videocache.Invalidate(cacheCtx, as.cache, id)
		cancel()
		if err != nil {
			log.Printf("改名后失效视频实体缓存失败: video=%d err=%v", id, err)
		}
	}
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
	refreshHash := auth.HashRefreshToken(refreshToken)
	if err := as.accountRepository.Login(ctx, account.ID, accessToken, refreshHash, refreshExpiry); err != nil {
		return "", "", err
	}
	if as.cache != nil {
		// 单会话互踢：上一轮的 refresh 反查键、重放暂存都必须清掉，不能只靠 TTL 等它过期
		if account.RefreshToken != "" && account.RefreshToken != refreshHash {
			as.mustClear(ctx, as.cache.Key("refresh:%s", account.RefreshToken))
		}
		if account.PrevRefreshToken != "" {
			as.mustClear(ctx, as.cache.Key("refresh:%s", account.PrevRefreshToken))
			as.mustClear(ctx, as.cache.Key("refresh:issued:%s", account.PrevRefreshToken))
		}
		// 当前生效 token 换成新的；写不进去就把快路径删掉，绝不留旧值。
		// 这一步做不干净，旧设备就踢不掉（旧值命中 == 旧 token 被判活放行）。
		as.setCurrentToken(ctx, account.ID, accessToken)
		as.setRefreshCache(ctx, account.ID, refreshHash, auth.RefreshTokenTTL)
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
		as.mustClear(ctx, as.cache.Key("account:%d:refresh", accountID))
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
		as.mustClear(ctx, as.cache.Key("refresh:issued:%s", account.RefreshToken))
	}
	if account.PrevRefreshToken != "" {
		as.mustClear(ctx, as.cache.Key("refresh:%s", account.PrevRefreshToken))
		as.mustClear(ctx, as.cache.Key("refresh:issued:%s", account.PrevRefreshToken))
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

// RefreshResult 一次刷新换出的凭证。
// RefreshToken 是明文，只出现在响应里；落库和 Redis 反查键用的都是它的哈希。
type RefreshResult struct {
	AccessToken  string
	RefreshToken string
	AccountID    uint
	Username     string
}

// refreshReuseGrace：轮换后旧 refresh 的宽限期。
//
// 宽限期内旧票再次出现，优先解释为「客户端没收到响应 / 并发刷新」，
// 用同一份新凭证幂等重放，避免弱网把用户踢下线；
// 超过宽限期再次出现，才判定为泄漏并吊销整个账号。
//
// 代价要讲清楚：这 60 秒内，拿到旧票的攻击者也能换到当前凭证。
// 窗口刻意取短，且旧票本来就和当前票存在同一处（同一客户端存储），边际风险有限。
const refreshReuseGrace = 60 * time.Second

func refreshCacheTTL(expiry time.Time) time.Duration {
	ttl := time.Until(expiry)
	if ttl <= 0 || ttl > auth.RefreshTokenTTL {
		return auth.RefreshTokenTTL
	}
	return ttl
}

func withinRefreshGrace(rotatedAt time.Time) bool {
	return !rotatedAt.IsZero() && time.Since(rotatedAt) <= refreshReuseGrace
}

// setRefreshCache 写 refresh 的两个影子键：
//   - account:{id}:refresh 值存哈希，登出/改密时对称删除；
//   - refresh:{hash} 反查 accountID，让刷新走 O(1)。
func (as *AccountService) setRefreshCache(ctx context.Context, accountID uint, refreshHash string, ttl time.Duration) {
	if as.cache == nil {
		return
	}
	cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()
	if err := as.cache.SetBytes(cacheCtx, as.cache.Key("account:%d:refresh", accountID), []byte(refreshHash), ttl); err != nil {
		log.Printf("failed to set refresh cache: %v", err)
	}
	if err := as.cache.SetBytes(cacheCtx, as.cache.Key("refresh:%s", refreshHash), []byte(strconv.FormatUint(uint64(accountID), 10)), ttl); err != nil {
		log.Printf("failed to set refresh lookup: %v", err)
	}
}

// stashIssuedRefresh 在宽限期内暂存「刚换出的新 refresh 明文」。
// 库里只有哈希、无法还原明文，没有这一份就无法对「丢了响应的重试」做到幂等；
// TTL 与宽限期一致，过期后随 Redis 一起消失。
func (as *AccountService) stashIssuedRefresh(ctx context.Context, prevHash, rawToken string) {
	if as.cache == nil {
		return
	}
	cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()
	if err := as.cache.SetBytes(cacheCtx, as.cache.Key("refresh:issued:%s", prevHash), []byte(rawToken), refreshReuseGrace); err != nil {
		log.Printf("failed to stash issued refresh: %v", err)
	}
}

func (as *AccountService) issueAccess(ctx context.Context, account *Account) (string, error) {
	token, err := auth.GenerateToken(account.ID, account.Username)
	if err != nil {
		return "", err
	}
	if err := as.accountRepository.UpdateToken(ctx, account.ID, token); err != nil {
		return "", err
	}
	as.setCurrentToken(ctx, account.ID, token)
	return token, nil
}

// replayRotated 处理宽限期内的旧 refresh：不再次轮换，直接返回上一次的结果。
// access 优先复用库里那张（未过期且可解析），避免每次重试都把上一张顶掉。
func (as *AccountService) replayRotated(ctx context.Context, account *Account, prevHash string) (*RefreshResult, error) {
	if as.cache == nil {
		return nil, ErrRefreshInvalid
	}
	cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
	b, err := as.cache.GetBytes(cacheCtx, as.cache.Key("refresh:issued:%s", prevHash))
	cancel()
	if err != nil || len(b) == 0 {
		// 暂存已过期/丢失：无法还原新 refresh，只能让客户端重新登录。
		return nil, ErrRefreshInvalid
	}

	access := account.Token
	if claims, perr := auth.ParseToken(access); access == "" || perr != nil || claims.AccountID != account.ID {
		var aerr error
		access, aerr = as.issueAccess(ctx, account)
		if aerr != nil {
			return nil, aerr
		}
	}
	return &RefreshResult{
		AccessToken:  access,
		RefreshToken: string(b),
		AccountID:    account.ID,
		Username:     account.Username,
	}, nil
}

// RefreshAccessToken 用 refresh token 换一张新的 access token，并轮换 refresh。
//
// 链路：
//  1. 主路径走 Redis 反查键 refresh:{sha256(token)} -> accountID（O(1)，且带剩余寿命 TTL）；
//  2. Redis 未命中 / 不可用时降级 MySQL，走 refresh_token / prev_refresh_token 索引；
//  3. 无论走哪条路，最终都以 MySQL 里存的值（哈希）为准做比对并校验过期时间；
//  4. 命中当前票 → CAS 轮换；命中上一张 → 宽限期内幂等重放，宽限期外按泄漏吊销；
//  5. 成功后回写 account:{id}，让后续鉴权继续走 Redis 快路径。
func (as *AccountService) RefreshAccessToken(ctx context.Context, refreshToken string) (*RefreshResult, error) {
	if refreshToken == "" {
		return nil, ErrRefreshInvalid
	}
	presented := auth.HashRefreshToken(refreshToken)

	// 1) Redis 主路径：反查键直接给出 accountID
	var account *Account
	if as.cache != nil {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		b, err := as.cache.GetBytes(cacheCtx, as.cache.Key("refresh:%s", presented))
		cancel()
		if err == nil {
			if id, parseErr := strconv.ParseUint(string(b), 10, 64); parseErr == nil && id != 0 {
				if acc, dbErr := as.FindByID(ctx, uint(id)); dbErr == nil {
					account = acc
				}
			}
		}
	}

	// 2) Redis 不可用/未命中：降级 MySQL 索引查询（同时匹配 current 与 prev）
	if account == nil {
		acc, err := as.accountRepository.FindByRefreshToken(ctx, presented)
		if err != nil {
			return nil, ErrRefreshInvalid
		}
		account = acc
	}
	if account.RefreshToken == "" {
		return nil, ErrRefreshInvalid
	}

	switch presented {
	case account.RefreshToken:
		if account.RefreshTokenExpiry.IsZero() || time.Now().After(account.RefreshTokenExpiry) {
			return nil, ErrRefreshExpired
		}

		newRaw, err := auth.GenerateRefreshToken(account.ID)
		if err != nil {
			return nil, err
		}
		newHash := auth.HashRefreshToken(newRaw)
		rotated, err := as.accountRepository.RotateRefresh(ctx, account.ID, presented, newHash, time.Now())
		if err != nil {
			return nil, err
		}
		if !rotated {
			// 并发刷新：另一个请求刚把这张票换掉，重读后确认是否「刚刚发生的轮换」。
			fresh, ferr := as.accountRepository.FindByID(ctx, account.ID)
			if ferr == nil && fresh.PrevRefreshToken == presented && withinRefreshGrace(fresh.RefreshRotatedAt) {
				return as.replayRotated(ctx, fresh, presented)
			}
			return nil, ErrRefreshInvalid
		}

		access, err := as.issueAccess(ctx, account)
		if err != nil {
			return nil, err
		}
		ttl := refreshCacheTTL(account.RefreshTokenExpiry)
		as.mustClear(ctx, as.cache.Key("refresh:%s", presented))
		as.setRefreshCache(ctx, account.ID, newHash, ttl)
		as.stashIssuedRefresh(ctx, presented, newRaw)
		return &RefreshResult{
			AccessToken:  access,
			RefreshToken: newRaw,
			AccountID:    account.ID,
			Username:     account.Username,
		}, nil

	case account.PrevRefreshToken:
		if withinRefreshGrace(account.RefreshRotatedAt) {
			return as.replayRotated(ctx, account, presented)
		}
		// 重用检测：旧 refresh 在宽限期外再次出现，按泄漏处理 —— 整个账号的凭证全部作废。
		log.Printf("SECURITY: refresh token reuse detected for account %d; revoking all tokens", account.ID)
		if err := as.revokeTokens(ctx, account); err != nil {
			return nil, err
		}
		as.mustClear(ctx, as.cache.Key("refresh:%s", presented))
		as.mustClear(ctx, as.cache.Key("refresh:issued:%s", presented))
		return nil, ErrRefreshReuse

	default:
		return nil, ErrRefreshInvalid
	}
}
