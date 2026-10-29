一、双进程与降级
P1｜降级只做了一半：API 启动时 Redis/RabbitMQ 连接失败会把 cache/rmq 置 nil，但路由和服务层没有统一的 nil 保护，后续请求可能直接 panic；而且没有后台重连，依赖恢复后必须重启进程。见 
backend/cmd/main.go
。
P1｜“双进程”名不副实：OutboxPoller、MQ Consumer、Notification worker 全部在 API 进程启动（
router.go:203-204
、
router.go:206-252
），Worker 进程只跑 Social/Like/Comment/Popularity 四类 worker（
backend/cmd/worker/main.go
）。API 仍然承担消费和定时任务，所谓“异步削峰/进程隔离”并不彻底。
P2｜Worker 启动策略偏硬：连接重试 10 次后直接 log.Fatalf，没有优雅停机、在途消息处理或重连；API 降级启动后 Worker 可能因 MQ 不可用直接退出。
二、Feed 三级缓存
P0/P1｜L3 回源是 N+1：GetVideoByIDs 对每个 miss 的 ID 起一个 goroutine、每条 SQL 查一次，而不是批量 IN；缓存大面积失效时会瞬间打出大量单行查询。
feed/service.go:36-146
P1｜singleflight 捕获首个请求的 ctx：首个调用者取消后，所有等待同一 key 的请求会一起拿到错误或空结果；而且错误被吞掉，调用方很难区分“没数据”和“回源失败”。同上。
P1｜没有空值缓存，GetDetail 也没有 singleflight：不存在的视频会反复穿透到 DB；热点 key 失效时多个请求同时回源，锁等待只有 100ms，超时后直接打 DB。
video_service.go:107-192
P1｜空 ZSET 重建走递归：ListLatest 在 ZSET 为空时重建，且忽略 ZAdd 错误；空库或 Redis 写入失败时存在反复递归的风险，而不是明确返回空。
feed/service.go:149-200
P1｜关注流缓存 24h，且缓存了 is_liked：点赞/取消赞不会失效关注流列表，点赞状态最多陈旧 24 小时。
feed/service.go:338-425
P1｜发布视频不失效粉丝的关注流缓存：新作品最多 24h 不可见，和“发布后粉丝立刻能看到”的预期冲突。
social/service.go
P2｜本地缓存无法跨进程失效：go-cache 3s/5s 只在本进程有效，Redis 失效不会清本地缓存；多实例下“自愈”完全依赖 TTL。
feed/service.go:33
P2｜游标单位不统一：latest 用毫秒、following 用秒，limit 兜底 10；前端要分别处理，接口契约比较脆弱。
feed/handler.go:109
三、冷热分离时间线与 Outbox
P1｜Outbox 没有状态机/租约：1 秒轮询 pending 100 条，发布成功直接 Delete，没有 SELECT FOR UPDATE、状态位或租约。多 API 实例会重复发布；发布后删除失败会重复投递。
outboxworker.go
P1｜没有 publisher confirm：PublishJSON 发出就认为成功，broker 未持久化或连接断开时，Outbox 行已经删了但消息丢了，时间线缺数据。
rabbitMQ.go
P1｜Outbox 依赖 MQ 初始非 nil：API 降级启动时 poller 无法工作，消息只积压、不恢复；也没有“MQ 恢复后补发”的机制。
P2｜ZSET 只保留 1000 条：深分页要回源 DB，热区与冷区拼接的边界容易重复/漏项；热榜/时间线的“冷热分离”只覆盖了前 1000 条。
P2｜时间线消费者在 API 进程：与“双进程”叙事不一致；API 重启或扩容会直接影响时间线构建。
四、滑动窗口热榜
P0 ✅已修｜DB 回退分页是坏的：前端 listByPopularity 只传 limit/as_of/offset，不传 DB 回退 cursor，Redis 不可用时热榜的“加载更多”会重复第一页。修复：① 前端保存并回传 next_latest_popularity/latest_before/latest_id_before 三元组；② 后端降级路径增加保护——offset > 0 且游标缺失时返回空 + has_more=false，绝不带着空游标从头查。
feed/service.go:427-533
、
frontend/src/api/feed.ts
、
frontend/src/components/HotRankPanel.vue
P1 ✅已修｜offset 按 items.length 推进：视频被删除时 len(items) < len(members)，推进少了会让下一页重复一条。已改为按 len(members) 推进，has_more 同理。（原描述中“DB 只按点赞数排序”有误：repo.go 实际按 popularity DESC, create_time DESC, id DESC 排序。）
P1｜快照过期后 offset 语义断裂：下一页按新的 as_of 重建快照，但没有版本校验；快照被淘汰时用户会看到“翻页前后不是同一份榜”。
P1｜PopularityWorker 吞掉 Redis 错误后照样 Ack：Redis 故障期间热度事件静默丢失，热榜不会“最终一致”。
popularityworker.go
、
popularity_cache.go
P2｜分钟桶合并成本随窗口线性增长：每次请求都 ZUNIONSTORE 多个分钟 key，没有合并结果缓存或窗口上限保护。
P2 ✅已修｜首页「热」tab 已改为真正的滑动窗口热榜（HotRankPanel 调 listByPopularity），点赞榜接口已整体删除；推荐/关注/热榜/话题四个 tab 并列。
useVideoFeed.ts
五、事件驱动与最终一致
P0｜LikeWorker 不是事务，重试会永久丢计数：插点赞行、更新计数、发热度事件不在一个事务；如果插行成功、计数失败，重试时 duplicate 直接跳过，计数再也补不回来；3 次失败后 Ack 丢弃，不进 DLX。
likeworker.go
P1｜CommentWorker 没有幂等键：消息重投递会重复插入评论；重试后同样 Ack 丢弃。
commentworker.go
P1｜SocialWorker 关注幂等不完整：重复插入关注时忽略 1062，后续粉丝数/缓存更新可能重复或状态不一致。
socialworker.go
P1｜DLX 绑定是错的：每个 <queue>.dlx 都用 # 绑定到同一个 DLX exchange，死信会被路由到所有 DLX 队列，可能被错误的消费者处理；GetRetryCount 也没被使用。
dlx.go
、
rabbitMQ.go
P1｜NotificationWorker 重试后 Ack、无去重：重投递会重复生成通知；mention 通知还绕过了这个 worker。
notificationworker.go
P2｜所谓“指数退避重试”是进程内阻塞：没有 jitter，失败 3 次直接丢；这套机制更接近“尽力一次”，不是真正的最终一致。
六、分片上传 / 断点续传
P0 ✅已修｜并发上传丢位图更新：原实现 UploadChunk 读-改-写整个 session JSON，前端 MAX_CONCURRENT=3 时后写覆盖前写。修复：进度从 session JSON 里剥离，改用真实 Redis 位图 `chunk_upload_bits:{upload_id}`，置位走 `SETBIT`（单 bit 原子写，与 PEXPIRE 合并成一次 pipeline 往返）；读取用 `GETBIT` pipeline / `BITCOUNT`。并发 3 片同时完成不再互相覆盖。
chunk_handler.go
、
chunk_entity.go
、
redis/cache.go
P1 ✅已修｜Init 不校验参数一致性：新增 `InitChunkUploadRequest.Validate()`，强制 `total_chunks == ceil(file_size/chunk_size)`、`chunk_size ≤ ChunkSize(5MB)`、`file_size ≤ 200MB`、`total_chunks ≤ 4096`、file_hash 必须是 32 位 hex、filename 不含路径分隔符；续传命中旧会话时还要比对 `Matches(req)`，参数变了就当陈旧会话重建。UploadChunk 侧新增序号越界校验与该下标期望长度校验（`ExpectedChunkSize`），长度不符直接 400。
chunk_handler.go
、
chunk_entity.go
P1 ✅已修｜Complete 不校验整文件 MD5：合并时用 `io.MultiWriter(文件, md5)` 流式算整文件 MD5，与 init 声明的 `file_hash` 比对，不符则删半成品并返回 400（expected/actual）。合并不再是“只信位图”。
P1 ✅已修｜并发/重复 complete 会重复合并：新增 `SETNX` 合并锁（TTL 10min，409 表示正在合并）+ `chunk_upload_result:{upload_id}` 结果缓存（TTL 1h）。重复的 complete 直接回放同一 URL，不再是 404；加解锁后重新读一次会话与位图，避免用旧快照判断。清理顺序改为「先写结果 → 再删位图/会话/hash 映射」，Redis 删除失败也不会留下“假完成”的脏会话。
P1 ✅已修｜truncate 写入可能让合并读到半片：`os.Create` 直接写目标分片会留下“文件被截成 0 字节再慢慢写”的窗口，另一个标签页的 complete 可能读到半片。改为先写 `{idx}.part` 再 `os.Rename` 原子替换（Windows 上 rename 不能覆盖，先 Remove 再改一次）。
P1 ✅已修｜bit 与磁盘不一致会死锁：`bit=true` 但临时文件丢失时（tmp 被清 / 容器重建 / 多实例磁盘不共享），重传被短路、complete 永久 500，会话卡死到 TTL 过期。修复分三层：① upload 幂等短路前 `os.Stat` 核对文件大小，不一致就清位重收；② 新增 `reconcileBits` 作为「位图 vs 磁盘」的唯一收敛点，`init` / `status` / `complete` 三个出口全部改走它（不再用 `chunkBits` 直接读位图、判满也不用裸 `BITCOUNT`），位是 1 但字节不在的当场 `SETBIT 0` 退回未上传；③ `mergeChunks` 先全量 Stat 一遍再动手，一次性返回**全部**缺失下标（`missing_chunks`）而不是拼到第一片坏了就 return，前端收到清单后自动补传并重试合并。这样一来刷新页面重传这类入口也能自愈，不再依赖「客户端刚好重试到那一片」。临时目录路径统一由 `h.tmpDir/chunkPath` 给出，避免两套规则漂移。
chunk_handler.go
、
VideoView.vue
P2 ✅已修｜ChunkSize=5MB 常量没被使用：init 现在强制 `chunk_size ≤ ChunkSize`，前后端契约由后端兜底；前端 `CHUNK_SIZE` 与后端 `video.ChunkSize` 注释互相对齐。
P2 ✅已修｜randHex 错误被忽略：init 里 `randHex(16)` 的错误不再丢弃，失败直接 500；失败时也不会再创建半截会话。
P2 ⚠️部分修正｜断点续传体验：原描述“整文件 MD5 全量读内存”不准确——`computeFileMD5` 是按 2MB 切片流式 append 的，不占整文件内存；真正的代价是 SparkMD5 同步计算占住主线程，大文件会短暂无响应（已加“计算文件指纹”提示，仍未引入 Web Worker，属已知取舍）。前端已补：重试改为指数退避 + jitter、4xx 不再无意义重试、单片 120s 超时（client.ts 新增 `timeoutMs`/AbortController）、合并前调 `chunk/status` 按服务端位图补传最多 3 轮、complete 遇 409 退避重试。upload_id 归属校验后端本已实现（`session.AccountID != accountID → 403`），原描述有误。
VideoView.vue
、
client.ts
、
video.ts
P2｜仍未处理｜无整体取消/暂停按钮：本轮只加了超时与补传，没有真正的 AbortController 取消入口与跨会话恢复（刷新后需重新选同一文件，靠 file_hash 命中旧会话）。
七、双 Token 与鉴权
P0/P1 ✅已修｜Refresh 回退是全表扫描：Redis 未命中后走 FindAll 全表扫，refresh 频率一高就是 DB 灾难；回退成功路径还不更新 account:<id> 的 access token 缓存，后续请求持续回源。修复：① 新增 `FindByRefreshToken`（`refresh_token` 加二级索引）替代全表扫描；② 两条路径统一在成功后回填 `account:<id>`；③ 删除了没人用的 `AccountService.FindAll`（它会把全表密码哈希读进内存）。
account/service.go
、
account/repo.go
P0/P1 ✅已修｜Refresh Token 的「7 天」只在 Redis 侧成立：MySQL 只存裸字符串，没有过期时间，Redis 宕机或 key 被淘汰后 refresh 变成永久有效。修复：`Account` 增加 `refresh_token_expiry` 列，登录时按 `auth.RefreshTokenTTL` 写入，刷新时两条路径都校验；`AccessTokenTTL/RefreshTokenTTL` 抽为常量，Redis TTL 与 DB 过期同源。
account/entity.go
、
auth/jwt.go
P0 ✅已修｜Redis 残留旧值时 MySQL 吊销不生效（登出/改密/互踢全部可绕过）：判活是「Redis 里的值 == 传入 token」就短路 return true，**完全不查 MySQL**；而所有写 Redis 的路径（登出的 DEL、登录/刷新/改名的 SET）都是 best-effort，失败只打日志。两者叠加的结果是：MySQL 已经吊销（token 被清空或覆盖），但 Redis 里仍躺着旧 token → 旧 token 继续被放行，最长到那个 24h TTL 到期。同一漏洞的另一面是「单会话互踢失效」：新设备登录时若 SET 新 token 失败、旧值还在，旧设备照样在线。修复：① 抽出 `setCurrentToken`，写失败即回退删除（只有「写进新值」或「key 不存在」两种可接受结果）；② 抽出 `mustClear`，DEL → 重试一次 → 写空哨兵三级降级，保证 Redis 不再持有可放行的旧值；③ 会把 `accounts.token` 改掉的五条路径（Login / Refresh / Rename / Logout / 改密）全部收敛到这两个 helper；④ `isActive` 回填 TTL 从固定 24h 改为「token 剩余寿命」，把理论残留窗口压到 ≤15 分钟；⑤ `Logout` 的重复登出提前 return 分支补上 Redis 清理，避免上一次清理失败的脏值赖着不走；⑥ `isActive` 显式排斥空值（哨兵），防止将来被改成允许空 token。
middleware/jwt/jwt.go
、
account/service.go
（对照：refresh 侧没有这个洞，因为 RefreshAccessToken 无论走 Redis 还是 MySQL 最后都强制回 MySQL 比对 accounts.refresh_token —— access 侧的「命中即放行」短路才是根因）
P1 ✅已修｜Redis 与 MySQL 不一致时误杀合法 token：`check()` 发现 Redis 里的值 != 传入 token 就直接 401，登录时若 Redis 写失败而 MySQL 写成功，用户会拿着刚发的合法 token 被判死。修复：不一致时改为回源 MySQL 再判（MySQL 才是权威，登出时两侧都删，吊销依然即时），通过后再回填 Redis。
middleware/jwt/jwt.go
P1 ✅已修｜SoftJWTAuth 语义错误：携带非法/过期 token 时直接 401，而不是按匿名继续；用户带着过期 token 访问公开 feed 会整页失败。修复：任何一步拿不到可信身份（未携带 / 头部格式错 / 解析失败 / 已吊销）都退化成匿名 `c.Next()`，绝不阻断公开内容；并抽出 `VerifyToken` 供 SSE 复用。
middleware/jwt/jwt.go
P1 ✅已修｜JWT_SECRET 未配置时随机生成：重启/多实例后 token 全部失效；部署配置里也存在弱默认值。修复：改为 fail-fast——`auth.Init()` 在 `cmd/main.go` 启动时校验，缺失直接 `log.Fatalf`；顺带用 `sync.Once` 修掉密钥全局变量的数据竞争（旧代码并发首次调用会 race）。compose 里的弱默认值也已换成「必须覆盖」的长串，长度不足 32 字节会打 WARNING。
auth/jwt.go
、
cmd/main.go
、
docker-compose.yml
P1 ✅已修｜改密接口未认证、无限流：/account/changePassword 不要求登录，知道用户名就能暴力尝试旧密码。修复：路由移进 `protectedAccountGroup`，账号身份改由 access token 注入（请求体不再收 username），并加 5 次/小时/账号限流；改密成功后复用统一的 `revokeTokens`（Redis + MySQL 双侧清理）。
router.go
、
account/service.go
、
account/handler.go
P2 ✅已修｜前端 isLoggedIn 只看 token 存在：过期 token 仍显示已登录，路由守卫也只看本地 token，直到下一次 API 401 才清理。修复：`isLoggedIn` 改为「有 token 且 exp 未到」，并在 store 内起 30s 心跳让 computed 随时间推进；改密成功后主动 `clearTokens()`。
auth.ts
、
ChangePasswordView.vue
、
router/index.ts
P1｜Refresh token 不轮换、无 reuse detection：7 天 refresh 被窃后可反复换 access，直到登出/改密。（已知取舍，暂不修；若要修需引入 refresh token 家族表 + 重放检测）
P2｜头像上传只校验扩展名：不校验 MIME/内容；若静态站允许 SVG/HTML，存在存储型 XSS 风险。
account/handler.go
P2｜刷新接口自身无限流：/account/refresh 是匿名接口且每次都要查库，建议补 IP 维度限流。（本次未修）
P2｜头像上传只校验扩展名：不校验 MIME/内容；若静态站允许 SVG/HTML，存在存储型 XSS 风险。
account/handler.go
八、限流、SSE、私信
P0｜nginx 后面的 IP 限流是“全站共享桶”：Gin 设置了 SetTrustedProxies(nil)，不信任任何代理，ClientIP() 拿到的是 nginx 容器 IP，而不是真实用户 IP。结果是登录/注册/点赞限流被全站共享，一个人就能把所有人的额度打满；日志里的 IP 也全错。
router.go:24
、
nginx.conf
P1｜限流器 Redis 故障时放行：固定窗口边界还能双倍突发；没有“账号 + IP”组合，代理池可绕过。
ratelimit.go
P1 ✅已修｜SSE 鉴权不查吊销：SSERequireAuth 只 ParseToken，不查 Redis/DB；登出、改名、改密后旧 token 仍能收 SSE，直到 15 分钟过期。修复：SSEHub 注入 cache + accountRepo，握手时复用 `jwt.VerifyToken`，与 HTTP 中间件同一套判活规则。（仍是握手时校验一次，长连接期间过期不会主动踢；Hub 是进程内 map，多实例下用户收不到或收不全——未修）
ssehub.go
P1｜前端完全没接 SSE：没有 EventSource，也没有调用通知未读数/标记已读接口；SSE、通知列表在后端是“死代码”，用户感知不到实时通知。
AppShell.vue
、
MessageView.vue
P1｜私信接口缺少边界：发送不校验接收者、无长度限制、无限流；List 固定 50 条；IsRead 未见更新逻辑；没有实时推送。
message/handler.go
P2｜nginx 没有为 SSE 关缓冲：只有 /static/ 设了 proxy_buffering off，SSE 只能走 /api/，默认缓冲会让事件成批到达/延迟，代理读超时也可能断流。
nginx.conf
九、前端契约与体验
P1｜粉丝/关注/作品数用列表长度：后端列表 limit 200，超过 200 后数字永远显示 200。
AccountView.vue
、
UserProfileView.vue
P2｜normalizeAccount 丢弃 avatar/bio：个人资料页信息丢失，头像/简介加载不出来。
P2｜改密成功后不清理本地 token：页面跳回登录页，但 isLoggedIn 仍可能为 true，直到下一次 401。
ChangePasswordView.vue
P2 ✅已修｜热榜“加载更多”在 Redis 不可用时重复第一页：同热榜 P0，前端已在 feed.ts / HotRankPanel.vue 回传 DB 回退 cursor。
feed.ts
、
HotRankPanel.vue
P2 ⚠️部分修正｜上传无整体超时/取消/自动续传：client.ts 已支持 `timeoutMs` + AbortController，分片上传已带超时与退避重试，合并前会自动按 status 补传缺失分片；仍缺“取消/暂停”按钮与会话级自动恢复。
client.ts
、
VideoView.vue
P2 ✅已修｜首页「热」tab 已是滑动窗口热榜；推荐/关注用各自的复合游标，热榜/话题各自维护分页状态，互不干扰。
useVideoFeed.ts
十、部署配置
P1｜代理头与信任配置互相矛盾：nginx 传了 X-Real-IP/X-Forwarded-For，后端却 SetTrustedProxies(nil)；两者必须一起改，否则限流、日志、审计全部失真。
P1｜JWT_SECRET 有弱默认值/未强制覆盖：生产环境应启动失败，而不是随机生成或使用示例值。
P2｜SSE 代理配置缺失：/api/ 没有 proxy_buffering off、没有更长的 read timeout。
P2｜缺少健康检查/优雅停机的证据：本次未核对 Dockerfile 和编排细节；如果确实没有，滚动更新会丢在途消息。
亮点与实现对照