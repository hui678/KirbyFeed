# 双 Token 鉴权面试问题汇总（20 问）

> 题源：`chatgpt的面试问题设计.md` 第七节「双 Token 鉴权」
> 对应实现：`backend/internal/auth/jwt.go`、`backend/internal/account/`、`backend/internal/middleware/jwt/jwt.go`、`backend/internal/http/router.go`、`backend/internal/worker/ssehub.go`、`frontend/src/stores/auth.ts`、`frontend/src/api/client.ts`
> 相关学习文档：`05-双Token鉴权与软硬鉴权.md`（本文在其基础上补全 20 问的完整口径）、`learn/02-账号模块与同步请求链路.md`

---

## 0. 先背这条链路（30 秒总述）

被问「讲讲你的双 Token 鉴权」时，先给这条，再按追问展开：

```
登录   ：bcrypt 验密 → 签 access(HS256 JWT, 15m) + 生成 refresh(32B 随机串, 7d)
         → MySQL 覆盖写 token / refresh_token / refresh_token_expiry（权威，一行=一个会话）
         → Redis 写 3 个影子 key（失败只 log，不阻断登录）

鉴权   ：ParseToken（签名 + exp，本地 CPU）
         → Redis GET account:{id}，命中且 == 传入 token → 放行
         → 未命中/不等 → 回源 MySQL 比对 accounts.token → 相等则回填 Redis
         → 都不成立：硬鉴权 401 / 软鉴权降级为匿名

刷新   ：Redis 反查 refresh:{token} → accountID（O(1)）
         → 未命中则 MySQL 走 refresh_token 二级索引
         → 两条路都必须回 MySQL 比对 + 校验 refresh_token_expiry
         → 签新 access → UpdateToken → 回填快路径

吊销   ：mustClear(DEL → 重试 DEL → 空哨兵) 清 3 个 Redis key → 清 MySQL 两列
         → 顺序「先 Redis 后 MySQL」，且 Redis 清理绝不允许 best-effort
```

**三句话内核：**

1. **JWT 解决「不用查库就知道你是谁」，解决不了「作废」** —— 要即时吊销，服务端就必须留状态；本项目的状态就是「当前生效的那一张 token」这一个比特。
2. **MySQL 是权威，Redis 是影子** —— 影子丢了可以回源，权威丢了不能。所以写入「先 MySQL 后 Redis」，读取「Redis 不一致就回源」，而**不一致时永远往「拒绝 / 回源」倒，绝不往「放行」倒**。
3. **软硬鉴权不是两套鉴权，是两种失败策略** —— 同一套 `isActive`，硬鉴权失败 401，软鉴权失败当匿名。

### 0.1 参数与 Key 总表（被问数字时张口就来）

| 项 | 值 | 位置 |
|---|---|---|
| Access Token TTL | 15 分钟 | `auth/jwt.go:20` |
| Refresh Token TTL | 7 天 | `auth/jwt.go:23` |
| Refresh Token 形态 | 32 字节 `crypto/rand` → 64 位 hex | `auth/jwt.go:96-102` |
| 签名算法 | HS256，解析时**显式校验 Alg** | `auth/jwt.go:113` |
| 密钥长度要求 | ≥ 32 字节（不足只告警）；缺失则**启动失败** | `auth/jwt.go:28,43-62` |
| 快路径 Key | `account:{id}` → 当前 access token | `service.go:174` |
| 快路径 TTL | **token 剩余寿命**（≤15m，上限 24h 兜底） | `middleware/jwt/jwt.go:160-174` |
| Redis 操作超时 | 50ms | `service.go:172`、`middleware/jwt/jwt.go:124` |
| refresh 反查 Key | `refresh:{token}` → accountID，TTL 7d | `service.go:283` |
| refresh 对称删除 Key | `account:{id}:refresh`，TTL 7d | `service.go:147` |
| 限流：登录 / 注册 | 10 次/分/IP、5 次/时/IP | `router.go:32-33` |
| 限流：改密 | 5 次/时/账号 | `router.go:39` |

### 0.2 代码地图

| 文件 | 职责 |
|---|---|
| `auth/jwt.go` | 签发/解析 access、生成 refresh 随机串；两个 TTL 常量的**唯一来源** |
| `account/entity.go` | `Account` 表：`token`、`refresh_token`(index)、`refresh_token_expiry` |
| `account/repo.go` | 纯 SQL：`Login`（覆盖写=互踢）、`FindByRefreshToken`（索引）、`Logout`（清两列） |
| `account/service.go` | 业务规则：`Login` / `RefreshAccessToken` / `revokeTokens` / `setCurrentToken` / `mustClear` |
| `middleware/jwt/jwt.go` | `JWTAuth`(硬)、`SoftJWTAuth`(软)、`isActive`(判活)、`VerifyToken`(SSE 复用) |
| `http/router.go` | 决定哪些路由挂硬鉴权、哪些挂软鉴权、限流挂在哪 |
| `worker/ssehub.go` | SSE 握手复用 `VerifyToken`，避免长连接绕过吊销 |
| `frontend/.../client.ts` | 401 → 单飞刷新 → 重放原请求 |
| `frontend/.../auth.ts` | `isLoggedIn` 判 `exp` + 30s 心跳，提前表现过期 |

---

# 一、第一层：基础原理（Q1–Q5）

## Q1. JWT 是什么？为什么无状态 JWT 在签发后通常不能被服务端直接撤销？

**【一句话】** JWT 是「签名过的自包含凭证」：服务端只验签名和 `exp` 就能得出「你是谁、有没有过期」，整个过程不查任何存储——所以也**没有任何地方记录「这张已经不要了」**。

**【展开】**

JWT 由三段 Base64URL 拼接：`header.payload.signature`。它提供三件事：

| 能力 | 来源 | 是否本地可判 |
|---|---|---|
| 身份 | `payload` 里的 `account_id` / `username` | ✅ 解码即得 |
| 完整性 | HS256 签名（HMAC-SHA256(header.payload, secret)） | ✅ 本地 CPU 运算 |
| 时效 | `exp` / `nbf` | ✅ 本地比对时间 |

缺的第四件事是**状态**：签名只证明「这张票是我签的、没被改过」，不证明「这张票现在还有效」。撤销一个已签发的 JWT，等价于要让所有验证方都忘掉一条它们从未记录过的信息——无状态系统里没有这个动作。

所以本项目的回答不是「JWT 不能撤销」，而是：**JWT 负责 1/2/3，第 4 项由服务端存「当前生效的那一张」来补**（`middleware/jwt/jwt.go:107-158`）。这就是「有状态 JWT」：身份仍然自包含，但多一次 O(1) 的 Redis GET 做判活。

**【边界 / 加分】**

- 严格说，无状态 JWT 也能通过**短 TTL** 把「撤销延迟」压到可接受范围（≤15 分钟）。如果产品接受这个窗口，可以完全不做判活。
- 本项目不接受：登出/改密/互踢都要求「立刻」。所以选择了「加一次 GET」而不是「等 15 分钟」。
- 反问加分句：**「JWT 不是不能撤销，是撤销的代价从『删一行状态』变成了『让所有验证方都知道』；在单服务 + 共享 Redis 的架构里，这个代价就是一次 GET。」**

---

## Q2. Access Token 和 Refresh Token 分别承担什么职责？为什么有效期不同？

**【一句话】** Access 是**通行证**（每次请求都出示，必须能快速验、泄漏损失小）；Refresh 是**换证凭证**（只在换证窗口出示，必须能吊销、能过期）。

**【展开】**

|  | Access Token | Refresh Token |
|---|---|---|
| 形态 | HS256 JWT（自包含） | 32 字节随机数 hex（**不透明**） |
| TTL | 15 分钟 | 7 天 |
| 谁校验 | 每个业务请求 | 只有 `POST /account/refresh` |
| 校验方式 | 本地验签 + Redis 判活 | 必须查 Redis / MySQL 比对 |
| 存哪 | MySQL `accounts.token` + Redis `account:{id}` | MySQL `accounts.refresh_token` + Redis `refresh:{token}`、`account:{id}:refresh` |
| 泄漏后影响 | ≤15 分钟，且**可被即时吊销** | 剩余生命周期内可持续换 access，直到登出/改密/互踢 |
| 失败表现 | 401（硬）/ 降级匿名（软） | 刷新失败 → 清空本地 token → 要求重新登录 |

**为什么 TTL 不一样**，本质是两种凭证的「泄漏成本」不同：

- Access 每次请求都在网络上飞，暴露面大 → TTL 必须短。短的代价是用户要频繁换证，所以必须有 Refresh。
- Refresh 暴露面小（只在换证时出现），但**一次泄漏能持续产 access**，所以它必须满足两个条件：**服务端可吊销** + **绝对过期**。本项目用 `refresh_token_expiry` 让「7 天」在 MySQL 侧也成立（`account/entity.go:11-14`），而不是只靠 Redis TTL。

**为什么 Access 用 JWT、Refresh 用随机串？** 因为两者的校验次数差三个数量级：Access 每个请求都验，值得为它省掉一次查库；Refresh 每次用**本来就要查库**（要判吊销、判过期），用 JWT 只是多一层签名校验，还容易被误当成「无状态凭证」乱传。

**【边界 / 加分】**

- 15 分钟 / 7 天不是定理，是体验与风险的折中：15 分钟 = 泄漏窗口；7 天 = 「一周不打开也不用重新输密码」的移动端惯例。
- 如果业务是后台管理，Access 可以压到 5 分钟；如果是 IoT 长连接，可能要重新设计（不能靠每次请求换证）。
- 加分句：**「Refresh 不是『更长的 Access』，它是权限更大、校验更严、暴露更少的另一种东西——所以它不应该出现在业务请求头里。」**

---

## Q3. 为什么不直接使用一个 7 天有效的 JWT？为什么不把 Access Token 也做成服务端 Session？

**【一句话】** 7 天 JWT 把「泄漏损失窗口」放大到 7 天且不可撤销；纯 Session 则把「每个请求的身份校验」变成一次必须命中的存储查询——两者都放弃了双 Token 想同时拿到的两个好处。

**【展开】**

**方案 A：一个 7 天 JWT**

| 问题 | 说明 |
|---|---|
| 泄漏不可撤销 | 攻击者拿到后 7 天内畅通无阻，除非你再引入服务端状态——那就不叫「纯无状态」了 |
| 权限变更滞后 | 改名/改角色/封禁要等 7 天 |
| 无法区分「换证」与「访问」 | 同一个凭证既能在业务接口用，又能在刷新接口用，等于把高价值凭证摊到最大暴露面 |
| 缓解手段只剩短 TTL | 短到 15 分钟，就必须有 Refresh —— 绕回双 Token |

**方案 B：Access 也用服务端 Session**

Session 的本质是「服务端存一份会话，客户端只拿 session_id」。它天然支持即时吊销，但代价是：

- 每个请求都要读会话存储（Redis/DB），**和本项目判活的那次 GET 成本一样**；
- 需要序列化/反序列化整个会话对象（或至少一个结构体），而 JWT 只需 base64 解码 + HMAC；
- 跨服务/跨语言时，Session 的格式和存储成为耦合点；JWT 的 claims 是标准格式；
- 移动端/多端场景下，Cookie 语义不总是可用（本项目前端把 token 放 `localStorage` 并显式加 `Authorization` 头）。

**本项目的真实取舍（要敢说）：** 判活已经要查一次 Redis，所以「JWT vs Session」的性能差距没想象中大。选 JWT 的真实收益是：**claims 自包含、签名校验是纯本地运算、刷新链路只需换 access 不动会话对象**。如果面试官追问「那你不还是查了 Redis」，标准回答见 Q9。

**【边界 / 加分】**

- 如果系统是「单一后端 + 已有成熟 Session 中间件」，用 Session 完全合理，不该为了简历硬上 JWT。
- 反过来说，如果系统要「无状态水平扩容 + 多语言服务」，JWT 的边际收益才明显。
- 加分句：**「双 Token 的价值不在『JWT 很酷』，而在把『高频低价值凭证』和『低频高价值凭证』分开，让前者可以短、后者可以严。」**

---

## Q4. JWT 的签名校验、过期校验和权限校验有什么区别？JWT 的 Payload 是否加密？

**【一句话】** 签名管「没被篡改」、`exp` 管「还没过期」、权限管「这个人能不能做这件事」——三者互不替代；而 Payload **只是 Base64URL 编码，不是加密**，任何人拿到 token 都能读出里面全部字段。

**【展开】**

| 校验 | 回答的问题 | 失败语义 | 本项目位置 |
|---|---|---|---|
| 签名校验 | 这张票是不是我签的？内容有没有被改？ | 401 / 软鉴权降级匿名 | `auth/jwt.go:109-121` |
| 过期校验 | 现在还在有效期内吗？ | 同上 | `exp` 由 jwt/v5 自动校验（`jwt.go:82`） |
| 判活（吊销） | 它还是「当前那一张」吗？ | 同上 | `middleware/jwt/jwt.go:116-158` |
| 权限校验 | 这个账号能不能调这个接口 / 操作这个资源？ | 403 或 401 | 路由分组 + service 层（见 Q18） |

**签名 ≠ 加密**：`eyJhY2NvdW50X2lkIjox...` 这段用 `base64url -d` 就能还原成 JSON。所以：

- 「JWT 里放用户 ID」没问题（本来就要给客户端看）；
- 「JWT 里放手机号、身份证、内部权限位」就有问题——**等于明文广播**；
- 要保密只能上 JWE（加密 JWT），本项目没用，也没必要。

**签名校验里最容易被忽略的一步：算法白名单。** 本项目显式比较 `token.Method.Alg() != HS256` 就拒绝（`auth/jwt.go:113`），这是防 `alg=none` 和「RS256 公钥当 HS256 密钥」这类算法混淆攻击的关键一行——只信任 `header.alg` 是漏洞。

**【边界 / 加分】**

- 本项目的 claims 只有 `account_id` / `username` / `exp` / `iat` / `nbf`，**没有角色和权限位**。好处是权限变更不需要重签 token（见 Q19），坏处是每个需要权限的接口要自己查。
- 本项目也**没有校验 `aud` / `iss`**。单服务单受众时问题不大；一旦拆成多服务共用密钥，就必须加，否则 A 服务的 token 能打 B 服务。
- 加分句：**「签名保证的是完整性，不是机密性；`exp` 保证的是时效，不是授权。把三者混成一句『JWT 校验过了所以可以操作』，是很多越权漏洞的起点。」**

---

## Q5. 为什么 Feed 需要支持匿名浏览？匿名用户与已登录用户的鉴权链路如何区分？

**【一句话】** 匿名浏览是产品需求（新用户冷启动、分享链接、SEO/爬虫），实现上不靠「两套中间件」，而靠**软鉴权：有合法身份就写上，没有或坏了就当匿名继续**，把「能不能匿名」交给路由决定。

**【展开】**

**为什么必须匿名可看**

- 新用户第一次打开 App，还没注册就要能看到内容，否则首屏是登录墙，转化率直接崩；
- 分享出去的视频链接，被分享者大概率没登录；
- 内容型产品的爬虫/预览卡（微信、搜索引擎）不带 token；
- 如果公开 Feed 因为 token 过期就 401，用户体验是「昨天还能刷，今天整页报错」——这正是旧实现的 bug。

**两条链路的区分点：路由挂哪个中间件**

```go
// router.go:179-191
feedGroup := r.Group("/feed")
feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))   // 公开列表：软
{
    feedGroup.POST("/listLatest", ...)
    feedGroup.POST("/listLikesCount", ...)
    feedGroup.POST("/listByPopularity", ...)
    feedGroup.POST("/listByTag", ...)
}
protectedFeedGroup := feedGroup.Group("")                  // 子组再挂硬
protectedFeedGroup.Use(jwt.JWTAuth(accountRepository, cache))
{
    protectedFeedGroup.POST("/listByFollowing", ...)       // 关注流必须登录
}
```

**软鉴权的三种输入，两种输出：**

| 输入 | `SoftJWTAuth` 行为 | 下游看到 |
|---|---|---|
| 没带 Authorization | 直接 `c.Next()` | 没有 `accountID` → `viewerAccountID = 0` |
| token 过期 / 签名非法 | 直接 `c.Next()`（**不是 401**） | 同上 |
| token 合法但已吊销 | 直接 `c.Next()` | 同上 |
| token 完全合法 | 写入 `accountID` / `username` | 非 0 → 查 `is_liked` / `is_following` |

下游 handler 的写法是固定三行（`feed/handler.go:41-45`）：

```go
viewerAccountID, err := jwt.GetAccountID(c)
if err != nil {
    viewerAccountID = 0        // 匿名：不报错，只是没有个性化字段
}
```

于是**一次软鉴权同时支撑了「匿名可浏览」和「登录态个性化」**：`viewerAccountID == 0` 就不查点赞/关注状态，非 0 才查。这样公开流只有一种代码路径，没有 if-else 分叉两套逻辑。

**【边界 / 加分】**

- 软鉴权不是「弱化安全」：它只用在**公开读**接口。所有写操作（点赞、评论、关注、上传、私信、登出、改密）都挂硬鉴权。
- 有一个刻意的降级后果：用户 token 过期但页面还开着，Feed 会变成匿名内容（看不到自己点过的赞）。前端用 `exp` 提前把登录态置为失效（`frontend/src/stores/auth.ts:35-43`），让「过期」在 UI 上先表现出来，而不是等用户发现 `is_liked` 全变 false。
- 加分句：**「软鉴权的语义是『身份是可选的，但一旦声称就必须是真的』——不能既带着过期 token 又假装自己已登录，也不能因为 token 坏了就把公开内容也关掉。」**

---

# 二、第二层：实现细节（Q6–Q13）

## Q6. Access Token 中包含哪些字段？`user_id`、`exp`、`iat`、`jti` 分别有什么用途？

**【一句话】** 本项目 claims 是 `account_id` + `username` + `exp` + `iat` + `nbf`，**没有 `jti`**——因为吊销用的是「服务端存当前 token 做白名单比对」，不需要给每张票编号。

**【展开】**

```go
// auth/jwt.go:65-86
type Claims struct {
    AccountID uint   `json:"account_id"`
    Username  string `json:"username"`
    jwt.RegisteredClaims        // ExpiresAt / IssuedAt / NotBefore
}
```

| 字段 | 用途 | 本项目的使用方式 |
|---|---|---|
| `account_id` | 主体标识 | 判活 key `account:{id}`、所有业务归属（作者/点赞人/评论人）都靠它；**不信任请求体里的 user_id** |
| `username` | 展示用 | 改名必须重签 token（`service.go:45-67` 的 `Rename` 就是「同事务改 username + 换 token」） |
| `exp` | 绝对过期 | jwt/v5 解析时自动校验；前端 `isLoggedIn` 也读它提前置灰（`auth.ts:35`） |
| `iat` | 签发时间 | 审计 / 排查；TTL 的计算基准 |
| `nbf` | 生效时间 | 本项目设为签发时刻，防止时钟偏差导致的「刚签发就可用」争议 |
| `jti` | **单张 token 的唯一 ID** | **本项目没有**。如果要用，典型场景：黑名单按 jti 存（只存被吊销的，TTL = 剩余 exp）、审计日志按 jti 关联一次会话、Refresh 轮换的 reuse detection |

**为什么本项目可以不要 `jti`？** 因为吊销模型是**白名单**而不是黑名单：

- 白名单（本项目）：服务端只记「当前有效的那一张」，判定 = 传入 token == 记录值。天然支持「顶号」（新 token 覆盖旧 token），不需要枚举历史 token。
- 黑名单：服务端记「所有被吊销但还没过期的 jti」。登出要写一条、TTL 到 exp、条目数 ≈ 最近 15 分钟的登出量。优点是纯无状态 JWT 也能加撤销，缺点是存储随吊销量增长、且无法表达「只允许最新一张」。

**代价（要主动说）：** 白名单模型下「一个账号同时只有一张有效 access」，所以不支持多端同时在线（见 Q20）。如果产品要求多端，`jti`（或 `session_id`）就变成必需品——因为吊销粒度要从「账号」细化到「会话」。

**【边界 / 加分】**

- 不要往 claims 里塞头像、昵称、粉丝数这类会变的数据：JWT 是不可变的，改了就得重签，而且会让每个请求都传输冗余数据。
- 加分句：**「`jti` 不是为了『唯一』而存在，它解决的是『吊销粒度』。当吊销的单位是账号时，比对『当前 token』就够了；当吊销的单位是设备/会话时，才需要 jti 或 sid。」**

---

## Q7. Refresh Token 是 JWT 还是随机不透明字符串？两种方式的优缺点是什么？

**【一句话】** 本项目用 **32 字节随机串的 64 位 hex**，不是 JWT——因为 Refresh 每次使用都必须查服务端，JWT 的「免查库」优势用不上，反而带来「看起来能离线验证」的误用风险。

**【展开】**

```go
// auth/jwt.go:96-102
func GenerateRefreshToken(accountID uint) (string, error) {
    b := make([]byte, 32)          // 256 bit 熵
    if _, err := rand.Read(b); err != nil { return "", err }
    return hex.EncodeToString(b), nil   // 64 个字符，正好塞进 size:64 的列
}
```

**参数注意**：函数签名留了 `accountID` 但没用到，说明设计上「refresh 不携带任何信息」——它就是一张**只能用来查表的号码牌**。

| 维度 | 不透明随机串（本项目） | Refresh 也用 JWT |
|---|---|---|
| 服务端能否吊销 | ✅ 天生只能靠查表，比对即吊销 | ⚠️ 仍需服务端状态，否则不可吊销 |
| 泄漏后能否伪造 | ❌ 不可伪造，只能原样使用 | ❌ 签名保护，但同理只能原样使用 |
| 每次使用成本 | 1 次 Redis GET / 1 次索引 SQL | 1 次验签 **+ 仍要查库/查 Redis** |
| 体积 | 64 字符 | 几百字节 |
| 能否携带信息（设备、家族） | ❌ 需要额外表 | ✅ 可以塞 claims |
| 误用风险 | 低（一看就是「要查表」） | **高**（容易被当成 access 到处传） |
| 实现复杂度 | 低 | 中（要管密钥、alg、过期） |

**关键判断：** 一个凭证「是否应该自包含」，取决于它的**验证次数**。Access 每请求都验 → 自包含收益大；Refresh 一天验几次 → 自包含毫无收益，只剩风险。

**本项目的存储现实（必须承认）：** `accounts.refresh_token` 存的是**明文**，走 `refresh_token` 二级索引等值查询（`account/repo.go:104-117`）。更稳的做法是存 `SHA-256(refresh_token)`：DB 泄漏时攻击者拿不到可用凭证，且哈希仍可建唯一索引。这不影响功能，是「纵深防御」的加分项。

**【边界 / 加分】**

- 随机串的熵必须够：32 字节 = 256 bit，暴力枚举不可行。若用 UUIDv4（122 bit）也可，但没必要省这 16 字节。
- 加分句：**「Refresh Token 的设计目标不是『少查一次库』，而是『必须查得到、查得到就能删』。所以它越长越随机越好，越不透明越好。」**

---

## Q8. 用户登录时，MySQL 与 Redis 分别保存什么？为什么 MySQL 是权威值、Redis 是影子值？

**【一句话】** MySQL 存「当前会话的三件套」（access、refresh、refresh 绝对过期），Redis 存的是它们的**可重建副本**；之所以说 MySQL 权威，是因为**Redis 里的每一个值都能从 MySQL 推出来，反之不成立**。

**【展开】**

登录路径（`service.go:115-155`）：

```text
FindByUsername → bcrypt 比对
  → 生成 access(15m) / refresh(7d)
  → repo.Login(): UPDATE accounts SET token=?, refresh_token=?, refresh_token_expiry=?   ← 先写权威
  → Redis(失败只 log): 
       DEL  refresh:{旧 refresh}      // 单会话互踢，旧反查键不能留
       SET  account:{id}         = access,   TTL = 15m
       SET  account:{id}:refresh = refresh,  TTL = 7d
       SET  refresh:{refresh}    = accountID, TTL = 7d
```

| 存储 | 内容 | TTL | 角色 |
|---|---|---|---|
| MySQL `accounts.token` | 当前 access | 无（用 JWT `exp` 表达） | **权威**：判活的最终依据 |
| MySQL `accounts.refresh_token` | 当前 refresh | 无 | **权威**：刷新的最终依据 |
| MySQL `accounts.refresh_token_expiry` | refresh 绝对过期 | 无 | **权威**：让「7 天」在 Redis 全挂时也成立 |
| Redis `account:{id}` | access 副本 | token 剩余寿命 | 影子：省一次 SQL |
| Redis `account:{id}:refresh` | refresh 副本 | 7d | 影子：登出时对称删除 |
| Redis `refresh:{token}` | accountID 反查 | 7d | **索引结构**，不是值副本：避免对 refresh 做全表扫描 |

**「权威」的严格定义（这是面试官真正想听的）：**

1. **可重建性**：Redis 清空后，系统行为不变，只是变慢。反之，MySQL 清空后，Redis 里的 token 无法复原出「这个账号还应该有哪些会话」。
2. **事务性**：MySQL 的一次 `UPDATE` 是原子的，是「会话切换」的唯一提交点。Redis 的三次写入不是原子的，中途失败会留下不完整状态——所以它不能当提交点。
3. **持久性**：Redis 可能被淘汰、重启丢数据、主从切换丢最近写。MySQL 有 redo log。

正因为如此，写路径的顺序是**先 MySQL 后 Redis**：如果反过来（先写 Redis 再写 MySQL 失败），就会出现「Redis 认为新 token 有效、MySQL 还是旧 token」，而 MySQL 才是那个「重启后还在」的东西。

**影子值的有效期设计：**

- `account:{id}` 的 TTL 是**token 剩余寿命**（`middleware/jwt/jwt.go:160-174`），不是固定 24h。理由是：这张缓存只用来判活，token 都过期了它就没意义；给更长的 TTL 只会延长「万一有脏值」的残留窗口。
- `refresh:{token}` 的 TTL 是 7d，和 `refresh_token_expiry` 对齐——**两边同源**（都从 `auth.RefreshTokenTTL` 派生），否则会出现「Redis 说过期、MySQL 说没过期」的裂缝。

**【边界 / 加分】**

- 严格说 Redis 的 `refresh:{token}` 不是「影子值」，而是**反查索引**：它的存在与否只影响性能，不影响正确性（MySQL 有索引可降级）。
- 加分句：**「判活不是『查 Redis 有没有』，而是『Redis 的值是否等于你手里的 token』。前者会把 Redis 变成信任源，后者只是把 Redis 当比较对象。」**

---

## Q9. 每次 API 请求都查询 Redis 验证 Token 状态，会不会抵消 JWT 无状态鉴权的性能优势？

**【一句话】** 会抵消一部分，但抵消的是「一次 SQL」，换来的是一次 O(1) 内存查询 + 即时吊销；而且这正好是「Session 方案本来就要付的成本」，所以并没有比 Session 更差。

**【展开】**

把三条路径放在一起比（同机房量级，具体数字以压测为准）：

| 路径 | 典型耗时 | 网络往返 | 备注 |
|---|---|---|---|
| 纯 JWT（不判活） | 微秒级 | 0 | HMAC + JSON 解码，纯 CPU |
| 本项目：JWT + Redis GET | 亚毫秒级（0.2–1ms） | 1 次 | 超时上限 **50ms**（`middleware/jwt/jwt.go:124`） |
| 回源 MySQL 主键查 | 1–3ms+ | 1 次 | 只在缓存未命中/不一致时发生，命中后回填 |
| 纯 Session（Redis） | 亚毫秒级 + 反序列化 | 1 次 | 与本项目判活基本等价 |

**结论**：本项目的「有状态 JWT」在**稳态下就是一次 Redis GET**，和 Redis Session 成本同阶。真正的收益在别处：

1. 身份信息不需要序列化整个会话对象，claims 解码即可用；
2. 刷新链路只换 access，不动会话结构；
3. 服务可以无状态水平扩容（除了共享 Redis，没有本地会话）；
4. 签名在网关/边缘就能先挡掉一批伪造请求，不必打到存储。

**成本控制手段（本项目已做）：**

- **50ms 硬超时**：Redis 抖动时最多拖 50ms，然后走回源分支（`context.WithTimeout`）；
- **命中即返回**，不回源；
- **回填时 TTL ≤ token 剩余寿命**，避免长命脏值；
- 软鉴权接口在 Redis 异常时**降级匿名**而不是报错，公开流不受影响。

**如果 QPS 再高一个量级，怎么优化（要能说出来）：**

| 方案 | 效果 | 代价 |
|---|---|---|
| 进程内 L1 缓存 `account:{id}`，TTL 1s | 把 Redis GET 降到 1 QPS/账号 | **吊销最多延迟 1s**，与「即时」冲突 |
| 吊销时通过 Redis Pub/Sub 广播失效 | L1 也能做到近即时 | 需要订阅通道 + 多实例一致性 |
| 把「是否已吊销」改成短 TTL 的负缓存（只缓存 revoked） | 正常用户仍查 Redis，被吊销的立即命中 | 只省一半 |
| 网关层 JWT 预校验 + 内部 RPC 携带 uid | 边缘挡伪造，内部不再验 | 仅适用于可信内网 |

**【边界 / 加分】**

- 一定要主动说：**「有状态 JWT 是一个明确的取舍，我们在简历里不该把它包装成纯无状态；它买的是即时吊销，付的是一次 GET。」**
- 如果面试官问「那为什么不干脆用 Session」——因为 Session 也要这次 GET，而 JWT 还额外给了「claims 不需要存储格式约定」的好处。
- 加分句：**「性能问题不是『有没有一次 GET』，而是『这次 GET 失败时会发生什么』。我们把 50ms 超时 + 回源 + 软降级做完，这次 GET 才敢放在每个请求上。」**

---

## Q10. 用户修改密码后，如何让之前签发的 Access Token 立即失效？是修改版本号、维护 Token 黑名单，还是删除会话记录？

**【一句话】** 本项目的做法是「**清空服务端记录的当前 token**」——它等价于一个只有 1 个槽位的会话表；`token_version` 和 `jti` 黑名单是同一问题的另外两种解法，分别对应「多会话」和「纯无状态」两种场景。

**【展开】**

**本项目的实现（`service.go:74-97` → `revokeTokens`）**

```text
ChangePassword(accountID 来自 token, old, new):
  FindByID → bcrypt 比对旧密码          // 防止拿到 token 的人直接改密
  → bcrypt(new) → UPDATE password
  → revokeTokens(account):
        mustClear(account:{id})            // access 快路径
        mustClear(account:{id}:refresh)    // refresh 副本
        mustClear(refresh:{旧refresh})     // 反查键
        UPDATE accounts SET token='', refresh_token=''   // 权威落空
```

判活侧看到 `accounts.token == ""` 就拒绝（`middleware/jwt/jwt.go:136`），所以**下一次请求立即 401**，不需要等 15 分钟。

**三种解法的对比（这是本题的核心）**

| 方案 | 怎么做 | 撤销粒度 | 每次请求成本 | 适用 |
|---|---|---|---|---|
| **当前 token 比对**（本项目） | 服务端存「那一张」，不一致即拒 | 账号级（单会话） | 1 次 Redis GET | 单端登录、要求简单 |
| **token_version** | `accounts.token_version` + claims 里带 `ver`；改密时 `ver+1` | 账号级，**所有设备一起失效** | 必须读一次 ver（Redis 或 DB） | 多端登录 + 「全部登出」 |
| **jti 黑名单** | 登出时把 `jti` 写入 Redis，TTL=剩余 exp | **单张 token** | 1 次 Redis EXISTS | 想保留纯无状态 JWT + 精细撤销 |
| **删除会话行** | 独立 sessions 表，按 sid 删 | 单会话/单设备 | 1 次查询 | 多端 + 设备管理 |

**为什么改密必须吊销 refresh，而不只是 access？** 因为攻击者若持有 refresh，15 分钟后就能换一张新 access，改密等于白改。所以 `revokeTokens` 同时清两列——这是「改密即吊销」的完整含义。

**为什么改密接口本身要「登录态 + 限流」？** 旧实现把 `changePassword` 放在匿名组、用户名从请求体来，等于给了一个「无限次试旧密码」的接口。现在它挂在保护组（`router.go:53-59`），身份只从 token 取，并加 5 次/时/账号限流（`router.go:39`）。

**【边界 / 加分】**

- `token_version` 和「当前 token 比对」的成本**其实一样**（都要读一次服务端值），差别在语义：前者表达「第几代凭证有效」，天然支持多设备；后者表达「当前那唯一一张」。
- 黑名单的坑：条目数 ≈ 最近 15 分钟的登出量，且必须保证「Redis 挂了也不放行」——否则黑名单失效就等于没吊销。所以纯无状态 + 黑名单，实际也是「有状态」。
- 加分句：**「改密要失效的不是『密码』，是『用旧密码换来的凭证』。所以既要清 access（立刻），也要清 refresh（否则 15 分钟后复活）。」**

---

## Q11. 用户退出登录后，Refresh Token 如何失效？Access Token 是否也必须立即失效？

**【一句话】** Refresh 通过「清空 MySQL 两列 + 删 Redis 三个 key」失效；Access 在本项目**必须同时立即失效**，因为单会话模型下「当前 token」被清空，判活必然失败。

**【展开】**

**登出链路（`service.go:157-169`）**

```text
Logout(accountID):
  FindByID
  if token == "" && refresh_token == "" {      // 重复登出
      mustClear(account:{id})                  // 仍要清一次！上一次可能没清干净
      return nil
  }
  revokeTokens(account)                        // 清 Redis 三键 + 清 MySQL 两列
```

| 凭证 | 失效机制 | 生效时机 |
|---|---|---|
| Refresh | MySQL `refresh_token=''` → 刷新时比对失败；Redis 反查键删除 → 连查都查不到 | 立即 |
| Access | MySQL `token=''` → 回源判活失败；Redis `account:{id}` 删除 → 快路径未命中 | 立即 |

**「Access 是否必须立即失效」是一道设计题，两种答案都对，取决于产品：**

| 选择 | 做法 | 结果 | 适合 |
|---|---|---|---|
| **立即失效**（本项目） | 登出 = 吊销当前 token | 已登出的设备下一请求就 401 | 安全优先、单端登录 |
| **不主动失效** | 只吊销 refresh，access 等 15 分钟自然过期 | 登出后旧 access 还能用 ≤15 分钟 | 纯无状态、不想引入判活 |
| **折中** | access 保留 ≤60s「宽限」 | 避免「登出瞬间的并发请求」报错 | 体验优先 |

本项目选立即失效，因为判活的那次 GET 本来就要做，多加一次 `DEL` 是免费的；反之如果选了「不主动失效」，就不能在简历里写「即时吊销」。

**重复登出为什么还要清 Redis？** 这是 4.4-bis 那个坑的补丁：如果上一次登出的 `DEL` 失败（MySQL 已空、Redis 还留着旧 token），用户再点一次登出时，代码会走进「MySQL 已空 → 提前 return」分支。若这个分支不清 Redis，脏值就会一直留到 TTL 到期。所以 `service.go:162-167` 在 return 前补了一次 `mustClear`。

**【边界 / 加分】**

- 「登出」在服务端是**幂等**的：重复调用都应返回成功，不能因为「已经登出」而报错——否则前端重试逻辑会误判。
- 如果做了多设备（Q20），「登出」的语义要拆成「退出当前设备」和「退出所有设备」，前者删一行 session，后者还要 bump `token_version`。
- 加分句：**「登出不是『把本地 token 删掉』，那是客户端行为；服务端登出是把『这张票还有效』这个事实抹掉。」**

---

## Q12. Refresh Token 续期时，如何防止并发刷新导致多个有效 Token？是否使用 Token 轮换和旧 Token 重用检测？

**【一句话】** 本项目**不轮换 refresh**，所以「并发刷新」不会产生多个 refresh，只会产生多个 access——代价是「同一瞬间可能有两张 access 都被判活」，收益是刷新链路简单；轮换 + 重用检测是更安全但更复杂的方案，本项目明确标为未实现。

**【展开】**

**现状（必须先承认）**

```text
RefreshAccessToken:
  Redis 反查 refresh:{token} → accountID
  → 未命中则 FindByRefreshToken（索引）
  → 强制回 MySQL 比对 + 校验 refresh_token_expiry
  → 签新 access → UPDATE accounts.token=newAccess
  → setCurrentToken(account:{id}, newAccess)
  → 返回 newAccess（refresh 原样不动）
```

`service.go:264-325` 的注释明确写了这个取舍：**换 access 不换 refresh**，好处是前端不用处理「两个请求拿着新老 refresh 打架」的竞态；坏处是 refresh 泄漏后在剩余 7 天内持续可用，且**没有 reuse detection**。

**并发刷新会发生什么（诚实的推演）**

| 时序 | MySQL `token` | Redis `account:{id}` | 结果 |
|---|---|---|---|
| 请求 A 完成 | A | A | A 有效 |
| 请求 B 完成 | B | B | B 有效，A 失效 |
| B 的 DB 写先到、Redis 写后到（乱序） | B | A | **A 与 B 都可能被判活** |

最后一行是真实存在的窗口：`setCurrentToken` 是「最后写赢」，而 DB 与 Redis 的写入顺序不保证与请求完成顺序一致。影响有限（两张都是同一用户合法换来的），但它意味着「单会话只有一张 access」在并发刷新下不是严格不变量。

**缓解手段（本项目已做 / 未做）**

已做：

- 前端**单飞刷新**：`refreshPromise` 让并发的 401 共享同一次刷新（`client.ts:36-62`），同一标签页内不会重复刷新；
- 刷新后 `tryRefresh()` 把新 token 写回 store 并重放原请求；
- 刷新失败 → `clearTokens()`，避免死循环。

未做（要主动说）：

- 后端没有「同一 refresh 只允许换一次」的原子性；
- 没有 refresh 家族表，检测不到 reuse；
- `/account/refresh` 自身**没有限流**。

**如果要做轮换（标准方案，能讲出来就是加分）**

```text
sessions(id, account_id, refresh_hash, prev_hash, family_id, expiry, used_at, revoked)

refresh 时：
  1) 原子 CAS：UPDATE sessions SET prev_hash=refresh_hash, refresh_hash=new,
                 used_at=NOW() WHERE id=? AND refresh_hash=? AND used_at IS NULL
     → RowsAffected=0 说明这张 refresh 已经被用过（reuse）
  2) reuse 命中 → 把整个 family 标记 revoked（攻击者与用户都被踢，强制重登）
  3) 正常返回新 refresh + 新 access
  4) 并发窗口：允许「上一个 prev_hash」在 N 秒内幂等返回同一新 token
```

三个关键点：**CAS 而非先查后改**（否则两个请求都读到同一旧值）、**family 而不是单条**（否则攻击者用新 token、用户用旧 token，只能踢掉一边）、**短宽限期**（否则弱网下正常用户会被误判 reuse）。

**【边界 / 加分】**

- 不轮换 ≠ 不安全，但必须配上别的约束：短 refresh 生命周期 + 改密即吊销 + 单会话互踢（新登录会覆盖旧 refresh，攻击者的旧 refresh 立刻失效）。
- 加分句：**「轮换解决的问题是『refresh 被偷后你多久能发现』。不轮换就永远发现不了，只能等它过期或被主动登出。」**

---

## Q13. Redis 中的会话记录过期、被淘汰或者发生故障时，系统应该放行、拒绝还是回源 MySQL？

**【一句话】** **回源 MySQL**——不放行（那是把缓存当信任源），也不拒绝（那会把「Redis 抖动」放大成「全员掉线」）；只有 MySQL 也查不到时才拒绝。

**【展开】**

`isActive` 的判定树（`middleware/jwt/jwt.go:116-158`）：

```text
Redis GET account:{id}
  ├─ 命中且 == token            → 放行（不查 DB）
  ├─ 未命中（过期/淘汰/不存在）  ┐
  ├─ 值 != token（旧值/哨兵）    ├→ 回源 MySQL 比对 accounts.token
  └─ Redis 报错/超时             ┘     ├─ 相等（且非空） → 回填 Redis → 放行
                                        └─ 不等/为空/出错 → 拒绝
```

注意一个反直觉的点：**「Redis 里有值但不等于传入 token」也必须回源，不能直接拒。** 因为 MySQL 才是权威：登录时如果 Redis 写失败（OOM、网络抖动）而 MySQL 成功，Redis 里躺着的是上一张旧 token，此时用户的**新** token 会被误判。回源既救回合法用户，又不牺牲吊销（登出时两侧都删）。

| Redis 状态 | 行为 | 是否可用 | 退化代价 |
|---|---|---|---|
| 正常 | 快路径命中 | ✅ | 0 |
| key 过期 / 被淘汰 | 回源 MySQL | ✅ | 每请求 1 次主键 SQL |
| 值不一致（脏旧值） | 回源 MySQL，通过则回填 | ✅ | 1 次 SQL（首次） |
| Redis 整体不可用 | 每次回源 MySQL | ✅ | 鉴权 QPS 上限 = MySQL 主键查询能力 |
| Redis + MySQL 都不可用 | 拒绝 | ❌ | **fail-closed**，安全优先 |

**为什么「回源」是唯一正确答案（三种选择的对比）：**

| 选择 | 后果 |
|---|---|
| **放行**（把 Redis 未命中当有效） | Redis 一挂，所有已登出的 token 全部复活——吊销机制形同虚设 |
| **拒绝**（Redis 未命中即 401） | Redis 抖动 1 秒 → 全站用户被踢 → 比安全事件更严重的可用性事故 |
| **回源**（本项目） | 正确性由 MySQL 保证，可用性由「降级到更慢的权威源」保证 |

**【边界 / 加分】**

- 这个策略有一个前提：**MySQL 主键查询必须扛得住降级流量**。所以本项目给 Redis 设了 50ms 超时（避免请求堆积），但如果真要防「Redis 全挂 → DB 被打穿」，还需要**回源限流/熔断**（这部分本项目没做，要主动承认）。
- 有一个有意的例外：**限流器在 Redis 出错时选择放行**（`middleware/ratelimit/ratelimit.go` 中 `IncrementWithExpire` 出错即 `c.Next()`）。理由完全不同：限流是「防滥用」，不是「鉴权」，宁可少挡也不能因限流器故障挡掉全部业务。**这两个「放行」不能混为一谈，面试时一定要区分。**
- 分片上传会话位图则相反：Redis 独有状态，挂了就 503 快速失败（不产生脏会话）。
- 加分句：**「降级的方向不是统一的，是按『这份状态能不能从权威源重建』决定的：能重建就回源，不能重建就快速失败，只有纯防护性的组件才可以 fail-open。」**

---

# 三、第三层：故障与反问（Q14–Q20）

## Q14. 如果 MySQL 已经吊销了 Token，但 Redis 仍然缓存着旧状态，用户还能访问多久？如何保证你声称的即时吊销？

**【一句话】** 如果清理是 best-effort、判活又是「命中即放行」，旧 token 最长能用到**快路径 TTL 到期**（旧实现是 24 小时）——这就是本项目修掉的 P0；修法是让「Redis 里不可能残留一个还判得活的旧值」成为不变量，把最坏窗口压到 **≤15 分钟**（且正常路径为 0）。

**【展开】**

**先复现漏洞（`05` 文档里的推演，必须能背）**

```text
1. 登录              MySQL: token=T1    Redis: account:1=T1
2. 登出，DEL 失败     MySQL: token=""    Redis: account:1=T1     ← 脏值赖着
3. 客户端收到 200
4. 攻击者拿 T1 请求  → isActive: Redis 命中且相等 → return true  ❌
5. 一直放行到 24h TTL 到期
```

两个错误方向叠加才会出事：

- **写**：`if err := cache.Del(...); err != nil { log.Printf(...) }` —— 失败只打日志，然后照常清 MySQL 并返回成功；
- **读**：`if err == nil && len(b) > 0 && string(b) == tokenString { return true }` —— 命中即放行，根本不查 MySQL。

**写允许失败，读却全信它**，方向就错了。同一漏洞的另一面是**单会话互踢失效**：新设备登录时 `SET` 新 token 失败、Redis 还是旧值，旧设备照样在线。

**修法三件套（`service.go:178-230`）**

```go
// ① 写失败必须回退删除：只有两种可接受结果——写进新值，或清掉让后续回源
func (as *AccountService) setCurrentToken(ctx, accountID, token) {
    if err := cache.SetBytes(key, token, auth.AccessTokenTTL); err != nil {
        as.mustClear(ctx, key)
    }
}

// ② 三级降级保证旧值一定消失
func (as *AccountService) mustClear(ctx, key) {
    for i := 0; i < 2; i++ {           // DEL → 重试 DEL
        if err := cache.Del(ctx, key); err == nil { return }
    }
    cache.SetBytes(ctx, key, []byte{}, auth.AccessTokenTTL)  // 空哨兵
}

// ③ 快路径 TTL = token 剩余寿命（最后兜底，把最坏窗口从 24h 压到 15m）
```

**空哨兵为什么有效？** `isActive` 的条件是 `len(b) > 0`（`middleware/jwt/jwt.go:129`），空串必然不满足，一定穿透回源 MySQL；而 MySQL 已经被清空，所以必然拒绝（`middleware/jwt/jwt.go:136`）。

**为什么三级都失败也不怕？** Redis 的 `DEL` 报错只可能是连接级/集群级故障，不会出现「单 key 删不掉但能读」。真到那一步，`GET` 同样会失败 → 一样回源 MySQL → 一样拒绝。**两条路径的失败方向是一致的，不会一个失败另一个成功。**

**「即时吊销」要给出可验证的定义：**

> 从 MySQL 事务提交那一刻起，不存在任何能让这张旧 token 通过判活的 Redis 值。

逐场景验证：

| 场景 | 结果 |
|---|---|
| Redis 正常，DEL 成功 | GET 未命中 → 回源 → token 空 → 拒绝 ✅ |
| DEL 失败一次、重试成功 | 同上 ✅ |
| DEL 两次都失败、哨兵写入成功 | 值 = `""` → 不等 → 回源 → 拒绝 ✅ |
| DEL 和哨兵都失败 | Redis 整体不可用 → GET 也失败 → 回源 → 拒绝 ✅ |
| 登录时 SET 新 token 失败 | `setCurrentToken` 回退删除 → 旧值消失 ✅ |
| 重复登出 | 提前 return 前补清 Redis ✅ |

**仍然存在的边界（主动说，别等被问倒）：**

1. **Redis 主从异步复制**：刚 `DEL` 主节点，读落到从节点仍可能读到旧值 → 窗口 = 复制延迟。本项目单实例 Redis 不受影响；上主从后要么读写都走主，要么接受这个毫秒级窗口。
2. **快路径 TTL 15 分钟不是「零」**：它只是兜底上限，正常路径靠主动 DEL，不走 TTL。
3. **没有自动化测试**覆盖「Redis 写失败」场景（`05` 文档已列为未修项）——这是最该补的回归用例。

**【边界 / 加分】**

- 一句话总结：**「不一致时必须往『拒绝 / 回源』倒，绝不能往『放行』倒。宁可让用户多等一次 SQL，也不能让已吊销的凭证继续通行。」**
- 加分句：**「即时吊销的难点不是『删 Redis』，而是『怎么证明删干净了』。我们用的不是重试次数，而是一个不变量：Redis 里能出现的值只有『当前 token』『空』『不存在』三种。」**

---

## Q15. 如果 Redis 故障时为了保证可用性而放行请求，会不会让已经登出的用户继续访问？如果选择拒绝，又有什么影响？

**【一句话】** 两个极端都不可接受：放行 = 吊销失效，拒绝 = 全站掉线；本项目选**第三条路——降级回源 MySQL**，用「更慢但正确」换可用性，代价是数据库压力。

**【展开】**

| 策略 | 登出用户能否继续访问 | 可用性 | 结论 |
|---|---|---|---|
| **放行**（Redis 不可用即信任） | ✅ 能（持续到 token 自然过期） | 最高 | 吊销机制作废，安全事件 |
| **拒绝**（Redis 不可用即 401） | ❌ 不能 | 全站掉线 | 可用性事故 |
| **回源 MySQL**（本项目） | ❌ 不能 | 降级可用 | ✅ 正确性不降，只降吞吐 |

**为什么「回源」能同时保住两边：** 因为判活所需的**全部信息**在 MySQL 里都有（`accounts.token` 一个字段），Redis 只是它的缓存。这正是「MySQL 权威、Redis 影子」的设计回报——影子没了，权威还在。

**代价必须量化，否则会被追问：**

- 稳态：1 次 Redis GET（亚毫秒）；
- 降级：1 次 MySQL 主键查询（1–3ms，走 `PRIMARY`，`accounts` 表按账号量线性增长但主键查询近似 O(log n)）；
- 放大倍数：假设 Redis 分担了 95% 的鉴权查询，Redis 挂掉后 DB 鉴权 QPS ≈ 全站 QPS，可能瞬间 20 倍——**这才是真正的风险点**。

**所以「回源」不是免费的正确性，还需要配套（本项目做了/没做）：**

| 措施 | 状态 |
|---|---|
| 50ms 超时，避免请求堆积在 Redis | ✅ 已做 |
| 判活回填 Redis，恢复后自动回到快路径 | ✅ 已做 |
| 回源失败即拒绝（fail-closed） | ✅ 已做 |
| 数据库连接池上限 + 主键索引 | ✅ 依赖既有配置 |
| **回源限流 / 熔断 / 降级为「只读接口放行、写接口拒绝」** | ❌ 未做，要主动承认 |

**如果非要「Redis 全挂还要抗住」，正确的思路不是放行，而是分层：**

1. **公开读**（Feed 列表）本来就走软鉴权 → 匿名即可用，天然免疫；
2. **写操作**必须硬鉴权 → 宁可 503 也不能放过未授权写；
3. **进程内 L1 短缓存**（1–5s）+ Pub/Sub 失效广播 → 把回源放大倍数压下来，代价是吊销延迟 ≤ TTL。

**【边界 / 加分】**

- 加分句：**「可用性和安全性在这里不是二选一，而是选『降级到什么程度』。我们把 Redis 定位成缓存而不是依赖，所以它挂了只是变慢；如果它被定位成会话的唯一存储，那就只能在放行和拒绝之间选一个坏答案。」**
- 顺手区分：限流器的 fail-open 是有意的（少挡一次比全站 429 好），**不能拿它给鉴权的 fail-open 辩护**。

---

## Q16. 如果攻击者窃取了 Refresh Token，如何降低风险？Token 轮换、重用检测和会话撤销如何配合？

**【一句话】** 先靠「短生命周期 + 可吊销 + 单会话互踢」把损失限制住，再靠「轮换 + reuse detection + 家族撤销」把「发现被盗」变成可能；本项目做到了前三条，后三条是明确的未实现项。

**【展开】**

**威胁模型**：攻击者拿到 refresh 后，可以在 7 天内不断换 access，且**用户毫无感知**（因为 access 是新的、合法的）。

**本项目的现有防线**

| 防线 | 机制 | 局限 |
|---|---|---|
| 不透明随机串 | 256 bit，不可伪造、不可预测 | 防不了「被偷」 |
| 7 天绝对过期 | MySQL `refresh_token_expiry` 双保险 | 7 天窗口仍然很长 |
| 改密即吊销 | `revokeTokens` 清两侧 | 用户得先发现异常 |
| 登出即吊销 | 同上 | 同上 |
| **单会话互踢** | 新登录覆盖 `refresh_token` | 用户重新登录一次就能踢掉攻击者——**这是本项目最实用的一条** |
| 传输安全 | 依赖 HTTPS（部署层） | 本项目代码不强制 |

**缺失的三件套（要能讲清设计）**

1. **轮换（rotation）**：每次刷新返回新 refresh，旧的立即作废。这样攻击者一旦使用旧 refresh，就与用户的正常刷新形成「同一张票被用两次」的信号。
2. **重用检测（reuse detection）**：维护 `family_id`，检测到某张已被替换的 refresh 再次出现 → 判定为泄漏 → **撤销整个家族**（用户和攻击者都被踢，强制重登）。只撤销单张是不够的：攻击者可能已经拿着新 refresh 跑了。
3. **绑定（binding）**：把 refresh 与设备指纹 / UA / IP 段绑定，异常环境直接要求重登。注意 IP 会变（移动网络），只能作为风险评分，不能硬绑。

**配合关系（一句话链路）**

> 短 TTL 限制损失窗口 → 轮换制造「旧票再用」的信号 → reuse detection 把信号变成家族撤销 → 用户被迫重登 + 告警 → 攻击者手里的所有 refresh 一起失效。

**存储改进（顺带加分）**：`accounts.refresh_token` 目前存**明文**。改成存 `SHA-256` 后，DB 泄漏也拿不到可用凭证；且哈希列仍可建唯一索引，功能不变。

**前端侧的配合**：`client.ts:38-62` 的单飞刷新避免同一标签页并发刷新（否则自己就会制造「reuse」误报）；多标签页/多设备仍需后端容忍短宽限期。

**【边界 / 加分】**

- 不轮换不是「错」，而是「用简单换安全」：本项目用单会话互踢兜住了最现实的场景（用户发现异常 → 重新登录 → 攻击者的 refresh 立刻失效）。
- 加分句：**「没有 reuse detection 时，refresh 泄漏是静默的；有了之后，它才变成一个可告警的事件。这两者的差别不在防御强度，而在可观测性。」**

---

## Q17. 如何防止 JWT 被篡改、算法混淆、过期时间设置错误，以及 Refresh Token 重放？

**【一句话】** 篡改靠签名 + **显式算法白名单**；过期靠 jwt 库的 `exp/nbf` 校验 + 常量化的 TTL；重放无法在协议层根除，只能靠「短 TTL + 传输安全 + 不把 token 放进 URL + 轮换」把窗口压小。

**【展开】**

**① 篡改与算法混淆（本项目已做）**

```go
// auth/jwt.go:109-118
jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
    if token.Method == nil || token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
        return nil, errors.New("unexpected signing method")   // 拒绝 alg=none / RS256 冒充
    }
    return secret, nil
})
```

- `alg=none` 攻击：不校验算法的实现会接受「无签名」token；
- 算法混淆攻击：把 RS256 公钥当 HS256 的 HMAC 密钥用，攻击者可自行签名；
- **显式比较 `Alg()` 是唯一可靠的防线**，不能只依赖库的默认行为。

**② 密钥管理（本项目已修）**

- 旧的「缺失就随机生成」会导致：重启后所有 token 失效 + 多实例之间互相 401；
- 现在是 `Init()` **fail-fast**（`auth/jwt.go:43-46`），并用 `sync.Once` 消除数据竞争（`jwt.go:30-34`）；
- 短于 32 字节只告警不拒绝——这是刻意的演示环境友好，生产应视为错误。

**③ 过期时间设置错误（本项目已做）**

- TTL 只有两个常量来源（`AccessTokenTTL` / `RefreshTokenTTL`），Redis TTL 和 MySQL `refresh_token_expiry` 都从它派生，禁止各处写死 `7*24*time.Hour`；
- 旧 bug：MySQL 只存裸字符串，「7 天」只在 Redis 侧成立，Redis 一挂 refresh 就变成永久有效 → 补 `refresh_token_expiry` 列；
- 时钟问题：`nbf` = 签发时刻，多实例时钟漂移大时需要 NTP；本项目没有引入时钟偏移容忍（`leeway`），要承认。

**④ Refresh Token 重放（部分缓解）**

| 手段 | 本项目 | 说明 |
|---|---|---|
| 短生命周期 | ✅ 7 天 | 压低窗口 |
| 一次性使用（轮换） | ❌ 未做 | 根治重放的手段 |
| reuse detection | ❌ 未做 | 把重放变成可检测事件 |
| 与 access 隔离（只在一个接口出现） | ✅ | 减小暴露面 |
| 传输安全 | 依赖 HTTPS | Refresh 绝不能出现在 URL/日志里 |

**⑤ 一个容易被忽略的真实风险：token 进了 URL**

SSE 握手允许 `?token=`（`worker/ssehub.go:84`），因为浏览器 `EventSource` 不能自定义 Header。后果：token 会出现在 Nginx access log、代理日志、Referer 里。更稳的做法是**先用 HTTP 换一张一次性、30 秒有效的 SSE ticket**，再用 ticket 建连。

**⑥ 前端存储（要敢说取舍）**

本项目把 access/refresh 放 `localStorage`（`stores/auth.ts:9-19`），好处是刷新页面不掉登录；坏处是一旦 XSS，token 直接被读走（`httpOnly` Cookie 读不到）。更稳的组合：**refresh 放 `httpOnly + Secure + SameSite` Cookie，access 只放内存**。本项目是前后端分离 + 显式 `Authorization` 头，选了简单方案。

**【边界 / 加分】**

- 加分句：**「JWT 的安全边界不在算法，而在三件事：算法白名单、密钥不落代码、TTL 有唯一来源。前两条防伪造，第三条防『以为过期了其实没有』。」**
- 本项目没有校验 `aud` / `iss`，单服务可接受，多服务共用密钥时必须补。

---

## Q18. 匿名用户可以浏览 Feed，但不能点赞或评论。这个权限边界是在 Gin 中间件、Handler 还是业务服务层实现的？

**【一句话】** 分三层：**中间件管「你是谁、这个路由要不要登录」，Handler 管「身份缺失时怎么补齐参数」，Service 管「这个资源你能不能动」——三层各管一件事，不能合并。**

**【展开】**

| 层 | 回答的问题 | 本项目实现 |
|---|---|---|
| **鉴权（AuthN）** | 你是谁？凭证有效吗？ | `JWTAuth` / `SoftJWTAuth` → 写 `accountID` / `username` |
| **粗粒度授权** | 这个接口要不要登录？ | **路由分组**：公开组 vs 保护组（`router.go`） |
| **资源级授权** | 这条数据是不是你的？ | **Service 层**：如 `comment_service.go:107` 的 `comment.AuthorID != accountID` |

**为什么「要不要登录」放在中间件/路由，而不是 Handler 里写 if？**

- 路由表本身就是一份**可审计的权限清单**：看 `router.go` 就知道哪些接口是公开的；
- 放 Handler 里，新增接口时容易忘；
- 中间件可以统一处理 401 的响应格式、统一做判活、统一被 SSE 复用（`VerifyToken`）。

**为什么「资源归属」必须放 Service，而不能放中间件？**

中间件执行时**还不知道要操作哪个资源**（`video_id` 在 body 里，`comment_id` 在 body 里）。中间件最多能判断「已登录」，判断不了「这条评论是不是你的」。所以：

```go
// comment_service.go:99-110（示意）
func (s *CommentService) Delete(ctx, commentID, accountID uint) error {
    comment, err := s.repo.FindByID(ctx, commentID)
    if err != nil { return err }
    if comment.AuthorID != accountID {          // ← 资源级授权
        return errors.New("permission denied")
    }
    ...
}
```

**为什么 Handler 层要做「身份缺失补 0」？**

因为软鉴权放行时不会写 `accountID`，`GetAccountID` 会返回 error。Handler 必须显式把它翻译成 `viewerAccountID = 0`（`feed/handler.go:41-45`），而不是把 error 往上抛——否则匿名用户刷 Feed 会变成 500。这一行是「软鉴权」和「业务个性化」之间的接缝。

**完整边界表**

| 操作 | 路由 | 中间件 | 匿名结果 |
|---|---|---|---|
| 刷公开 Feed | `/feed/listLatest` 等 | 软 | ✅ 正常返回，无 `is_liked` |
| 刷关注流 | `/feed/listByFollowing` | 软 + 硬（子组） | 401 |
| 点赞 / 评论 / 关注 | `/like/*`、`/comment/publish`、`/social/*` | 硬 | 401 |
| 删自己的评论 | `/comment/delete` | 硬 + Service 校验归属 | 401 / 403 |
| 看通知（SSE） | `/notification/*` | 硬（`SSERequireAuth`） | 401 |

**【边界 / 加分】**

- 严格说本项目没有 403 语义，资源越权也返回 500/400 混用（`comment_service.go` 返回普通 error）——这是个可以改进的点：**认证失败 401、授权失败 403、参数错误 400**，语义分开对排查和网关策略都更友好。
- 加分句：**「中间件适合做『所有接口都一样』的判断，Service 适合做『每条数据都不一样』的判断。把归属校验塞进中间件，就得把业务主键解析逻辑搬进中间件，那才是真正的耦合。」**

---

## Q19. 如果用户被封禁、角色发生变化，JWT 中的旧权限信息如何失效？是否每次请求都需要查询服务端权限？

**【一句话】** 本项目**没有角色/封禁字段**，也**没有把权限塞进 JWT**——这正是它的好处：权限变更不需要等 token 过期；封禁要走「吊销」这条已有的路（清两侧记录），而不是加个字段就完事。

**【展开】**

**先分清两件事：**

| 变更 | 该走哪条路 | 原因 |
|---|---|---|
| **封禁 / 解封** | 吊销（清 Redis + MySQL token）或引入 `status` 并在判活时检查 | 封禁的语义是「立刻不能再用任何凭证」 |
| **角色 / 权限变更** | 每次请求从服务端取（可缓存），不要放 JWT | 权限是易变状态，JWT 是不可变快照 |

**如果只加 `accounts.status='banned'` 而不清 Redis，会发生什么？**

```text
1. 管理员 UPDATE status='banned'
2. 用户请求 → isActive → Redis 命中且 token 相等 → return true   ❌ 封禁无效
3. 直到快路径 TTL（≤15m）到期才开始拒绝
```

因为快路径的短路**根本不查 MySQL**。所以封禁有两条正确做法：

1. **复用吊销**：`revokeTokens`（清三键 + 清两列）——封禁 = 强制下线，最彻底；
2. **把状态并入快路径**：`account:{id}` 的值从「token」改成 `token|status`（或另存 `account:{id}:status`），判活时一起比对——省一次 SQL，但引入了格式耦合。

**如果角色/权限放进 JWT 会怎样？**

- 优点：零查询，网关可直接判；
- 缺点：改权限必须等 token 过期（≤15m）或强制重签；`Rename` 就是「claims 变了必须重签」的现成例子（`service.go:45-67` 在**同一个事务**里改 username + 换 token，否则 JWT 里的 username 会和新用户名不一致）。

**本项目的实际选择：claims 只有身份，没有权限位。** 于是：

- 权限判断每次都在服务端做（本项目目前只做到「是否登录 + 资源归属」，没有 RBAC）；
- 角色变更天然即时（因为没有任何缓存的权限快照）；
- 代价是每个需要权限的接口都要查一次——本项目规模下可以接受。

**如果要加 RBAC，推荐的分层：**

| 权限数据 | 放哪 | 理由 |
|---|---|---|
| 角色/权限位 | Redis `acl:{account_id}`，TTL 5–10 分钟 | 变更少、可接受短延迟 |
| 高危操作（封禁、提权、资金） | 每次查 MySQL | 不容忍延迟 |
| 一般读权限 | 可放 JWT claims（TTL 内接受滞后） | 省查询 |

**【边界 / 加分】**

- 加分句：**「封禁不是『改一个字段』，是『让已发出的凭证失效』。只要你的鉴权有快路径缓存，任何『改库即生效』的想法都必须先回答：缓存里的旧值怎么办？」**
- 本项目没有 `status` 字段也没有管理员接口，这点要如实说，别把设计讲成已实现。

---

## Q20. 如果多设备登录，用户只退出当前设备时，如何避免误删其他设备的会话？如果用户选择全部设备登出呢？

**【一句话】** 本项目是**单会话模型**（一行 `accounts` 只存一张 token，登录即覆盖 = 互踢），所以「多设备同时在线」根本不支持；要支持就必须把 token 从账号行里拆出来变成**会话表 + `sid` claim**，这是结构性改动，不是加个字段。

**【展开】**

**当前模型的真实行为（先讲清楚，别硬吹）**

```text
A 手机登录  → accounts.token = TA, refresh_token = RA
B 电脑登录  → accounts.token = TB, refresh_token = RB   ← 覆盖，A 立刻被踢
A 再请求    → Redis 里是 TB（或回源看到 TB）→ 不等于 TA → 401
```

| 能力 | 是否支持 | 原因 |
|---|---|---|
| 多设备同时在线 | ❌ | 一行只存一张 token |
| 退出当前设备 | ❌（等于退出全部） | 没有「设备」这个维度 |
| 退出全部设备 | ✅（天然） | 清空就是全部 |
| 登录互踢 | ✅（天然） | 覆盖写 |

**为什么当初这么设计：** 一行一个会话最简单——判活只需要「一个 key 比一个字符串」，登出只需要「清一次」，没有会话表、没有 GC、没有「如何列出我的设备」。对短视频这类单端为主的场景够用。

**要支持多设备，改造成本在哪（面试官想看的是这个）**

```text
sessions(
  id            BIGINT PK,
  account_id    BIGINT,          -- 索引
  sid           CHAR(32) UNIQUE, -- 进 JWT claims
  refresh_hash  CHAR(64),        -- SHA-256(refresh)
  device        VARCHAR(128),    -- UA / 设备名
  last_seen_at  DATETIME,
  expires_at    DATETIME,        -- 绝对过期
  revoked_at    DATETIME NULL
)
```

配套改动：

| 环节 | 改动 |
|---|---|
| JWT claims | 增加 `sid`（会话 ID）；判活从「比对 token」变成「按 sid 查会话是否 revoked」 |
| 登录 | `INSERT` 一行新会话，不覆盖旧行 |
| 退出当前设备 | `UPDATE sessions SET revoked_at=NOW() WHERE sid=?` + 删 Redis `session:{sid}` |
| 退出全部设备 | `UPDATE ... WHERE account_id=?`（或 `token_version+1`） |
| 判活快路径 | Redis `session:{sid}` → 状态；未命中回源 sessions 表 |
| 清理 | 定时删过期会话（否则表无限增长） |
| 设备列表 UI | `SELECT ... WHERE account_id=? AND revoked_at IS NULL` |

**关键设计点：**

1. **`sid` 必须进 claims**，否则服务端无法知道「这张 access 属于哪个会话」；
2. **Redis key 按 sid 而不是按账号**，否则一个账号多会话会互相覆盖（就是现在的 bug 在多设备下的版本）；
3. **「退出全部」用 `token_version` 更省事**：claims 里带 `ver`，账号行 `ver+1`，所有旧 token 自动失效，不用逐行 UPDATE——代价是要多一次版本读取；
4. **refresh 也要按会话存**，不能用现在的 `accounts.refresh_token` 单列。

**改造成本评级：** 中等偏大。会影响 `entity`（新增表）、`service`（登录/刷新/登出全部重写）、`middleware`（判活逻辑换 key）、`router`（新增 `logoutAll`/`listDevices`）、前端（设备管理页）。不是「加个字段」能糊过去的。

**【边界 / 加分】**

- 面试时如果被问「你的方案不支持多设备，是不是缺陷」——正确回答是：**「这是明确的取舍：单会话换来了判活只需要一次字符串比较、登出只需要清一次。如果产品要支持多端，我会引入 sessions 表 + sid claim，成本我已经评估过。」**
- 加分句：**「多设备不是『允许多个 token』，而是『吊销粒度从账号细化到会话』。granularity 一变，Redis key 结构、claims、登出语义都要跟着变。」**

---

# 四、20 题一页速答

| # | 问题 | 一句话答案 |
|---|---|---|
| 1 | JWT 为何不能撤销 | 自包含 = 无状态记录；本项目用「服务端存当前 token」补上 |
| 2 | 双 Token 分工 | Access 15m 通行证；Refresh 7d 换证凭证，必须可吊销 |
| 3 | 为何不用 7 天 JWT / Session | 前者泄漏窗口 7 天且不可撤销；后者每次查会话，双 Token 把高频/高价值凭证分开 |
| 4 | 签名/过期/权限 | 签名=完整性，exp=时效，权限=授权；Payload 只编码不加密 |
| 5 | 匿名浏览 | 产品需要；软鉴权「有就认、没有当匿名」，`viewerAccountID=0` |
| 6 | Access claims | account_id/username/exp/iat/nbf；无 jti（白名单模型不需要） |
| 7 | Refresh 形态 | 32B 随机串 64 hex；必须查库，JWT 无收益 |
| 8 | MySQL vs Redis | MySQL 权威（可提交/持久/可重建），Redis 影子（丢了大不了回源） |
| 9 | 每次查 Redis 是否抵消优势 | 抵消一次 SQL，换一次 GET + 即时吊销；与 Session 同阶 |
| 10 | 改密吊销 | 清两侧当前 token（等价单槽会话）；版本号/jti 是另两种粒度 |
| 11 | 登出 | 清 MySQL 两列 + 3 个 Redis key；Access 本项目也立即失效 |
| 12 | 并发刷新 | 不轮换 → 无多 refresh，但可能短暂多 access；轮换+reuse detection 未做 |
| 13 | Redis 记录没了 | 回源 MySQL；MySQL 也失败才拒绝（fail-closed） |
| 14 | MySQL 已吊销 Redis 有旧值 | P0 坑；`mustClear` 三级 + 写失败回退删除 + TTL≤剩余寿命 → ≤15m |
| 15 | Redis 故障放行/拒绝 | 都不可取，回源；代价是 DB 放大，需限流熔断 |
| 16 | Refresh 被盗 | 短 TTL + 改密吊销 + 单会话互踢；轮换/reuse 未做 |
| 17 | 篡改/混淆/重放 | 显式 Alg 白名单 + fail-fast 密钥 + TTL 单一来源；重放靠短 TTL |
| 18 | 权限边界 | 中间件=身份与是否登录，Service=资源归属，Handler=匿名补 0 |
| 19 | 封禁/角色 | 无角色字段；封禁走吊销；权限不入 JWT |
| 20 | 多设备 | 单会话模型；要支持需 sessions 表 + sid claim |

---

# 五、面试官最可能追的 5 题：追问链

## 5.1 Q8（MySQL 权威 / Redis 影子）

```text
「MySQL 和 Redis 都存 token，以哪个为准？」
  → MySQL。定义：Redis 的值可从 MySQL 推出，反之不成立。
「那 Redis 里的值不等于传入 token，你直接拒绝吗？」
  → 不。回源 MySQL，相等则回填。否则「Redis 写失败」会变成「刚登录就被 401」。
「回填的 TTL 给多少？」
  → token 剩余寿命（≤15m），不是固定 24h——给长 TTL 只会扩大脏值窗口。
「为什么刷新要多一次 MySQL 比对，不直接信 Redis 反查键？」
  → 登出时 DEL 若失败，被吊销的 refresh 还能换 access。MySQL 比对是兜底。
```

## 5.2 Q9（一次 GET 值不值）

```text
「既然要查 Redis，为什么不直接用 Session？」
  → 成本同阶；JWT 还省了会话序列化和格式耦合。真正的差别是失败策略，不是性能。
「QPS 上去了怎么办？」
  → 进程内 L1 + 1s TTL，或 Pub/Sub 广播失效；代价是吊销延迟。
「能接受吊销延迟吗？」
  → 不能，所以本项目没做 L1。这是一个明确的取舍。
```

## 5.3 Q10（改密即时失效）

```text
「改密后旧 access 还能用吗？」
  → 不能。revokeTokens 清 Redis 三键 + MySQL 两列，下一请求判活失败。
「只清 MySQL 行不行？」
  → 不行。快路径命中旧值会直接放行，根本不查 MySQL。
「只清 Redis 行不行？」
  → 不行。Redis 故障期间请求回源到还没清的 MySQL，旧 token 又活了。
「顺序为什么是先 Redis 后 MySQL？」
  → 反过来会有「MySQL 已吊销、Redis 仍放行」的窗口。
```

## 5.4 Q12（并发刷新）

```text
「两个请求同时刷新会怎样？」
  → 都成功，各拿一张 access；MySQL 最后写的赢，Redis 也是最后写的赢，可能短暂不一致。
「会有安全问题吗？」
  → 两张都是同一用户合法换来的，风险有限；但它破坏了「单会话只有一张 access」的严格性。
「为什么不轮换 refresh？」
  → 前端不用处理新旧 refresh 竞态；代价是无法检测 reuse。要补就上家族表 + CAS + 宽限期。
```

## 5.5 Q14（即时吊销，最狠的一题）

```text
「MySQL 吊销了，Redis 还有旧值，用户还能访问多久？」
  → 旧实现最长 24h（P0）。现在正常路径 0，最坏 ≤15m。
「怎么保证的？」
  → 不变量：Redis 里只可能是「当前 token / 空串 / 不存在」。
     setCurrentToken 写失败回退删除；mustClear = DEL→重试→空哨兵；TTL 取剩余寿命。
「三级都失败呢？」
  → DEL 报错意味着连接级故障，GET 也会失败 → 一样回源 MySQL → 拒绝。
「怎么证明？」
  → 逐场景枚举（DEL 成功/重试成功/哨兵成功/全失败/写入失败/重复登出），每条都落到「回源后拒绝」。
「还有什么没兜住？」
  → 主从异步复制窗口（毫秒级）、没有自动化回归测试。
```

---

# 六、旧文档 / 常见说法纠错（哪些话现在不能背）

| 不能这么说 | 为什么错 | 应该怎么说 |
|---|---|---|
| 「JWT 是无状态的，所以性能好」 | 本项目每请求查 Redis 判活，不是纯无状态 | 「身份自包含、签名本地校验；吊销状态由 Redis GET 补上」 |
| 「Redis 存的是最新 token，以 Redis 为准」 | 权威在 MySQL，Redis 是影子 | 「MySQL 权威、Redis 快路径；不一致时回源」 |
| 「Redis 里的值不一样就拒绝」 | 会把「Redis 写失败」放大成「用户被踢」 | 「回源 MySQL，相等则回填」 |
| 「登出时删 Redis 失败只打日志」 | 这正是 P0 根因 | 「`mustClear` 三级降级，不允许残留可放行的旧值」 |
| 「快路径 TTL 24 小时」 | 残留窗口 24h，已修 | 「TTL = token 剩余寿命，上限 15 分钟」 |
| 「软鉴权就是把错误吞掉」 | 吞错和「降级为匿名」是两回事 | 「拿不到可信身份 → 匿名继续；公开读不受影响」 |
| 「我们支持多设备登录」 | 单会话模型，登录即互踢 | 「单会话；要支持需 sessions 表 + sid」 |
| 「Refresh 轮换防重放」 | 本项目没轮换 | 「不轮换，靠短 TTL + 改密吊销 + 互踢；reuse detection 未做」 |
| 「JWT 是加密的」 | Payload 只是 Base64URL | 「只签名不加密，不能放敏感信息」 |
| 「限流失败放行说明我们容错好」 | 那是有意的防护性组件策略 | 「限流 fail-open 是有意的；鉴权是 fail-closed，不能混」 |

---

# 七、不看答案自测（12 题）

1. Access 和 Refresh 的 TTL 各是多少？分别存在哪几个地方？
2. `account:{id}` 的 TTL 为什么不是 24h？改成多少、依据是什么？
3. 判活时 Redis 命中但值不相等，为什么不能直接 401？
4. 登录时 Redis 写入失败，用户还能登录成功吗？为什么？
5. `mustClear` 的三级分别是什么？空哨兵为什么一定导致回源？
6. 为什么改密要「先清 Redis 再清 MySQL」？
7. 刷新为什么不用 JWT？刷新为什么最后还要回 MySQL 比对一次？
8. 软鉴权遇到过期 token 返回什么？下游怎么知道这是匿名？
9. 多实例部署时，`JWT_SECRET` 不一致会表现成什么？现在的代码怎么防？
10. SSE 为什么不能只 `ParseToken`？`?token=` 有什么副作用？
11. 并发刷新可能造成什么不一致？为什么说风险有限？
12. 如果要做多设备登录，最小改动集合是什么？

---

# 附：一页数字记忆卡

```
Access 15m ｜ Refresh 7d ｜ 随机串 32B→64hex ｜ HS256 显式白名单 ｜ 密钥 ≥32B 且 fail-fast
快路径 account:{id} → TTL = token 剩余寿命（≤15m，兜底 24h 上限）
Redis 超时 50ms ｜ 缓存值只有三种：当前 token / 空串哨兵 / 不存在
刷新：Redis 反查 → MySQL 索引降级 → 强制 MySQL 比对 + refresh_token_expiry
吊销：mustClear(DEL→重试→空哨兵) × 3 key → MySQL 清两列（先 Redis 后 MySQL）
登录/注册/改密限流：10/min/IP、5/h/IP、5/h/账号
软鉴权：无 token / 过期 / 非法 / 吊销 → 全部匿名继续；硬鉴权 → 401
多设备：不支持（单会话，登录即互踢）；要支持 = sessions 表 + sid claim
```
