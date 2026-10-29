# Feed 流短视频系统学习文档

> 本文档基于项目真实代码编写，所有技术点均对应具体文件与行号，学习时可对照源码验证。
> 核心目录：`backend/internal/feed`、`backend/internal/video`、`backend/internal/worker`、`backend/internal/middleware`

---

## 一、模块概览

本系统实现了一个类抖音的短视频 Feed 流，核心解决 **高并发读、低延迟写、最终一致性** 三大问题。采用双进程架构：

- **API 进程**（`cmd/main.go`）：处理同步 HTTP 请求，Redis / RabbitMQ 均可降级运行
- **Worker 进程**（`cmd/worker/main.go`）：独立消费异步消息，依赖不可用时快速退出

核心功能模块：Feed 流（最新/关注/热榜/点赞排行）、视频发布、点赞/评论、关注、分片上传、鉴权、限流。

---

## 二、文件结构与职责

### Feed 模块（`backend/internal/feed/`）

| 文件 | 职责 |
|------|------|
| `handler.go` | HTTP 请求解析、参数校验、响应组装 |
| `service.go` | 缓存策略、分页编排、冷热数据路由 |
| `repo.go` | 纯 GORM 查询，无业务逻辑 |
| `entity.go` | 请求/响应结构体定义 |

依赖装配统一在 `internal/http/router.go` 中完成。

### 异步模块

| 文件 | 职责 |
|------|------|
| `internal/worker/likeworker.go` | 消费点赞事件，落库 + 计数 |
| `internal/worker/popularityworker.go` | 消费热度事件，更新 Redis 分钟桶 |
| `internal/worker/outboxworker.go` | Outbox 轮询投递 + 时间线消费 |
| `internal/middleware/rabbitmq/` | MQ 连接、声明、发布封装 |

---

## 三、三级缓存体系

### 3.1 链路设计

三级缓存逐层收敛，读路径为：**L1 本地内存 → L2 Redis → L3 MySQL**

```
请求进来
  ↓
L1: go-cache（秒级 TTL）命中？→ 直接返回
  ↓ 未命中
L2: Redis MGet 批量查询（50ms 超时）命中？→ 回写 L1，返回
  ↓ 未命中 / Redis 故障
L3: MySQL 查询 → 异步回写 L2 + L1，返回
```

**代码位置**：[feed/service.go#L35-L110](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/feed/service.go#L35-L110)

### 3.2 关键参数

- **L1 go-cache**：`cache.New(3*time.Second, 5*time.Second)`，实际写入 TTL 为 5 秒（[service.go#L32](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/feed/service.go#L32)、[service.go#L85](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/feed/service.go#L85)）
- **L2 Redis**：实体缓存 `video:entity` 为 1 小时，列表结果缓存 `feed:listByFollowing` 为 24 小时（`cacheTTL: 24 * time.Hour`）
- **L2 超时**：`context.WithTimeout(ctx, 50*time.Millisecond)`，超时即降级到 L3

### 3.3 并发防护：singleflight + SETNX 分层

按查询成本分层，不在同一链路混用：

| 缓存类型 | 并发防护 | 代码位置 |
|----------|----------|----------|
| 视频实体缓存 | `singleflight` 进程内去重 | `requestGroup` 字段，`f.requestGroup.Do(...)` |
| 关注流列表缓存 | `Redis SETNX` 互斥锁（跨实例） | [service.go#L381](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/feed/service.go#L381) |

**为什么分层**：实体缓存查询成本低，singleflight 足够；列表缓存涉及多表 JOIN，查询成本高，需要跨实例锁防止多实例同时回源。

**SETNX 锁实现**：
- 加锁：`redisClient.Lock(ctx, lockKey, 500ms)`，返回随机 token
- 解锁：Lua 脚本校验 token 后 `DEL`，防止误删其他持有者的锁
- 锁超时：500ms，避免死锁
- 未获取锁的请求：轮询 5 次 × 20ms 等缓存回填

### 3.4 降级与自愈

- Redis 不可用 / 超时：`missedL2 = missedL1`，全部降级到 MySQL（[service.go#L94-L97](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/feed/service.go#L94-L97)）
- 回写缓存使用**独立 context + goroutine**，不阻塞请求响应路径
- 恢复后由下一次请求的回源自动回填缓存

### 3.5 已知短板

- **缓存穿透**：不存在的视频 ID 查询结果不写入空值缓存，会反复穿透到 MySQL（无空值缓存/布隆过滤器）
- **ListByFollowing 的 context 复用**：锁获取与缓存回写复用 50ms 短超时 context，高并发下容易失败

---

## 四、冷热分离时间线

### 4.1 设计目标

最新视频流是全局时间线，写入量大、读取量更大。如果直接查 MySQL ORDER BY created_at LIMIT，会产生深分页性能劣化与新数据插入导致的翻页重复/遗漏。

### 4.2 写入路径：Outbox + MQ 异步写 ZSET

1. **发布视频**：视频记录与 Outbox 消息在**同一事务**写入 MySQL（`internal/video/video_service.go`）
2. **Outbox 轮询**：`StartOutboxPoller` 单协程，每秒查询 `status=pending` 的前 100 条，投递到 RabbitMQ（[outboxworker.go#L17-L47](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/worker/outboxworker.go#L17-L47)）
3. **消费写 ZSET**：`StartConsumer` 消费消息，`ZADD feed:global_timeline`，score 为视频创建时间毫秒戳（[outboxworker.go#L91-L96](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/worker/outboxworker.go#L91-L96)）

### 4.3 冷热边界：1000 条阈值

每次写入 ZSET 后执行：
```go
redisClient.ZRemRangeByRank(ctx, timelineKey, 0, -1001)
```
**只保留最近 1000 条**，旧数据（冷数据）从 ZSET 淘汰。（[outboxworker.go#L105](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/worker/outboxworker.go#L105)）

### 4.4 读取路径：冷热判定与缝合

1. 请求携带时间游标 `before`
2. 查询 ZSET 最老分数（第 1000 条的 score）
3. **热数据**：游标 > 最老分数 → `ZREVRANGEBYSCORE` 从 Redis 取
4. **冷数据**：游标 ≤ 最老分数 → 直接查 MySQL，**不回填缓存**（防止冷数据污染热点池）
5. **缝合**：热数据不足 limit 时，用最后一条视频的创建时间作为冷游标去 MySQL 补齐

### 4.5 分页方式

统一使用**时间游标分页**（`before` + `limit`），替代 OFFSET。游标为上一页最后一条的 `created_at`，解决深分页性能与新数据插入导致的重复/遗漏。

---

## 五、滑动窗口热榜

### 5.1 为什么不用实时排序

热度实时变化，如果每次查询都实时计算排序，翻页时榜单变化会导致跳页/重复。

### 5.2 写入：分钟分桶 ZINCRBY

热度更新不直接修改总榜，而是按分钟写入分桶：
```
hot:video:1m:202610231430  → ZINCRBY 视频ID +1
hot:video:1m:202610231429  → ZINCRBY 视频ID +1
...
```

- 写热点分散到 60 个 key，避免单个 key 成为瓶颈
- 分钟桶写入完成后**不可变**，保证历史数据稳定

**代码位置**：[video/popularity_cache.go#L20-L28](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/video/popularity_cache.go#L20-L28)

### 5.3 读取：ZUNIONSTORE 现场合并

```go
const win = 60
for i := 0; i < win; i++ {
    keys = append(keys, 最近第 i 分钟的桶 key)
}
dest := "hot:video:merge:1m:202610231430"  // 快照 key，含 as_of 时间
if !Exists(dest) {
    ZUnionStore(dest, keys, "SUM")     // 合并 60 个桶的 SUM
    Expire(dest, 2*time.Minute)        // 快照 2 分钟过期
}
ZRevRange(dest, offset, offset+limit-1)
```

**代码位置**：[feed/service.go#L435-L453](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/feed/service.go#L435-L453)

### 5.4 as_of 快照分页

- `as_of` 参数冻结榜单时间窗口（取分钟级时间戳）
- 翻页时复用同一个 `dest` 快照 key，保证翻页期间榜单稳定
- **取舍**：快照 2 分钟过期，窗口内榜单冻结；超过 2 分钟后翻页会拿到空结果，需重新拉取首页

### 5.5 已知问题

- **快照重建无并发保护**：`Exists → ZUnionStore` 之间没有锁/singleflight，并发首访会重复计算 60 窗口并集
- **热度为准实时**：经 MQ 异步写入，非强实时

---

## 六、事件驱动异步化与最终一致性

### 6.1 点赞双消息设计

一次点赞发两条消息，职责分离：

| 消息 | 路由 | 消费者 | 写入 | 性质 |
|------|------|--------|------|------|
| 落库事件 | `like.like` → `like.events` | LikeWorker | likes 表 + likes_count + popularity | 事实数据 |
| 热度事件 | `video.popularity.update` → `video.popularity.events` | PopularityWorker | Redis 分钟桶 + 删实体缓存 | 排序信号 |

**为什么不合并**：消费者职责不同，失败模式不同（MySQL 慢 vs Redis 快），独立队列隔离，且可分别降级。

**代码位置**：[like_service.go#L60-L72](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/video/like_service.go#L60-L72)

### 6.2 API 侧降级

```go
mysqlEnqueued := likeMQ.Like(...) == nil
redisEnqueued := popularityMQ.Update(...) == nil

if mysqlEnqueued && redisEnqueued {
    return nil  // 快速路径
}

// 落库失败 → 事务直写 MySQL
if !mysqlEnqueued {
    db.Transaction(func(tx) {
        tx.Create(like)
        tx.Model(&Video{}).UpdateColumn("likes_count", gorm.Expr("likes_count + 1"))
        tx.Model(&Video{}).UpdateColumn("popularity", gorm.Expr("popularity + 1"))
    })
}

// 热度失败 → 直写 Redis
if !redisEnqueued {
    UpdatePopularityCache(ctx, cache, videoID, 1)
}
```

**代码位置**：[like_service.go#L58-L105](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/video/like_service.go#L58-L105)

**两层降级**：
1. 启动时连不上 MQ → `likeMQ = nil`，所有请求走同步写入
2. 运行中 publish 失败 → 各自走 fallback 直写

**注意**：`amqp.Dial` 只连一次，MQ 恢复后 API 不会自动重连，需重启 API 进程。

### 6.3 Worker 消费与幂等

**连接与重连**（[worker/main.go#L43-L88](file:///d:/fronted-projects/feedsystem_video_go-main/backend/cmd/worker/main.go#L43-L88)）：
- `connectWithRetry`：指数退避（1s, 2s, 4s...上限 30s），最多 10 次，用尽后 `log.Fatalf` 退出
- `runWorkerWithRetry`：每个 Worker 独立 Channel，`Qos(50)` 预取，断连 5 秒后重连

**幂等消费**（[likeworker.go#L105-L130](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/worker/likeworker.go#L105-L130)）：
- 插入靠 `idx_like_video_account` 唯一索引去重（[like_entity.go#L7-L8](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/video/like_entity.go#L7-L8)）
- `LikeIgnoreDuplicate`：1062 错误返回 `created=false`，跳过计数
- `DeleteByVideoAndAccount`：`RowsAffected == 0` 表示已删，跳过

**重试策略**：
- 进程内指数退避重试 3 次（1s, 2s, 4s）
- 重试耗尽后 `Ack(false)` —— **实际是丢弃，不进死信**
- `ctx` 取消时 `Nack(false, true)` 退回队列

### 6.4 DLX 死信队列现状（重要）

**设计意图**：消费失败进死信不丢消息。
**实际代码**：DLX 已声明（[dlx.go](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/middleware/rabbitmq/dlx.go)），但 Worker 重试耗尽后执行 `Ack`，消息**永远不会进 DLX**。`like.events.dlx` 队列恒为空，`GetRetryCount` 是死代码。

**要让 DLX 生效**：需将兜底 `Ack(false)` 改为 `d.Reject(false)`。

### 6.5 关注流程的差异

关注**不是**事件驱动落库，而是：
1. 同步写 DB
2. 失效该用户的关注列表缓存
3. 发 MQ **仅用于通知**，失败只打日志不影响业务

**代码位置**：[social/service.go#L43-L56](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/social/service.go#L43-L56)

---

## 七、分片上传与断点续传

### 7.1 两段式设计

- **上传阶段**：分片落盘，返回 upload_id，不写数据库
- **发布阶段**：将 URL 与业务元数据在同一事务写入数据库

### 7.2 上传流程

1. **InitChunkUpload**：前端传 FileHash，后端查 Redis 索引：
   - 命中：返回已有 upload_id + 已传分片列表（断点续传）
   - 未命中：创建新会话，`UploadedBits` 位图记录分片状态
2. **UploadChunk**：接收分片，校验分片 MD5，写入 `UploadedBits`
3. **CompleteChunkUpload**：校验所有分片已上传，按序合并，删除会话与哈希索引

**代码位置**：[chunk_handler.go#L65-L134](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/video/chunk_handler.go#L65-L134)

### 7.3 关键参数

- **分片大小**：5MB（`5 << 20`）
- **会话 TTL**：24 小时
- **最大文件**：200MB

### 7.4 已知限制

- **强依赖 Redis**：`cache == nil` 时所有上传接口返回 503（[chunk_handler.go#L22](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/video/chunk_handler.go#L22)），无降级
- **无秒传**：FileHash 仅用于断点续传的会话索引，完成后即删除，不存在"文件已存在则直接返回 URL"的秒传功能

---

## 八、双 Token 鉴权与软硬鉴权

### 8.1 Token 设计

| Token | 有效期 | 存储 | 用途 |
|-------|--------|------|------|
| Access Token | 15 分钟 | JWT（无状态） | API 鉴权 |
| Refresh Token | 7 天 | MySQL + Redis | 刷新 Access Token |

**代码位置**：[jwt.go#L49](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/auth/jwt.go#L49)、[account/service.go#L139](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/account/service.go#L139)

### 8.2 单会话互踢与吊销

- Token 双写 MySQL 与 Redis
- 登出/改密：删除 Redis 中的 token，更新 MySQL
- 同一账号新登录会覆盖旧 token，旧 token 失效

### 8.3 刷新接口的降级

刷新流程：
1. 先查 Redis `refresh:{token}` 找 accountID
2. 查 DB 比对 `account.RefreshToken == refreshToken`
3. 生成新 Access Token，更新 DB 与 Redis

**降级路径**：Redis 不可用时，全表扫描 MySQL 比对 RefreshToken（[account/service.go#L227-L242](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/account/service.go#L227-L242)）

### 8.4 软硬鉴权

- **JWTAuth**：严格鉴权，无 token 或 token 失效直接 401
- **SoftJWTAuth**：软鉴权，无 token 也放行，解析成功则注入账号信息
- Feed 流使用 SoftJWTAuth，实现匿名可浏览、登录态下发个性化点赞/关注状态

**代码位置**：[router.go#L174](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/http/router.go#L174)

### 8.5 已知取舍

- 刷新时**轮换 Refresh Token**（CAS + 60s 宽限期幂等重放）：旧票只能用一次，宽限外重放触发整账号吊销；代价是前端必须保存轮换出的新票

---

## 九、固定窗口限流

### 9.1 实现

基于 Redis Lua 脚本原子执行：
```lua
local count = redis.call("INCR", KEYS[1])
if count == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return count
```

**代码位置**：[redis.go#L89-L95](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/middleware/redis/redis.go#L89-L95)

### 9.2 限流配置

| 接口 | 维度 | 阈值 | 代码位置 |
|------|------|------|----------|
| 登录 | IP | 10/min | [router.go#L32](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/http/router.go#L32) |
| 注册 | IP | 5/h | [router.go#L33](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/http/router.go#L33) |
| 点赞 | 账号 | 30/min | [router.go#L35](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/http/router.go#L35) |
| 评论 | 账号 | 10/min | [router.go#L36](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/http/router.go#L36) |
| 关注 | 账号 | 20/min | [router.go#L37](file:///d:/fronted-projects/feedsystem_video_go-main/backend/internal/http/router.go#L37) |

### 9.3 降级原则

- Redis 不可用时，限流器直接放行，不阻塞业务
- 计数维度无法获取时（如未登录无法获取账号），按 IP 降级或直接放行
- 超限使用 `c.AbortWithStatusJSON` 中断请求链

---

## 十、设计取舍总结

| 设计点 | 取舍 | 代价 |
|--------|------|------|
| L1 短 TTL（5s） | 平衡并发吸收与脏数据窗口 | 跨实例数据最长 5s 不一致 |
| 列表缓存 24h | 最短读路径 | 数据新鲜度低，依赖写路径失效 |
| Outbox 秒级轮询 | 发布事务强一致 | 时间线写入延迟 1s 内 |
| 热榜快照 2min | 翻页稳定 | 超窗翻页失效 |
| 点赞异步落库 | 接口低延迟 | 落库有延迟，靠唯一索引兜底 |
| 分片上传强依赖 Redis | 会话原子性 | Redis 故障时上传不可用 |
| Refresh 轮换 + 60s 宽限重放 | 旧票一次性使用，可检测重用 | 宽限期内无法区分弱网重试与攻击者；前端必须写回新票 |
| 固定窗口限流 | 实现简单 | 窗口边界可能出现 2 倍突发 |

---

## 十一、常见面试问题速查

1. **三级缓存如何防止缓存击穿？**
   - 实体缓存：singleflight 进程内去重
   - 列表缓存：Redis SETNX 互斥锁 + 锁内二次确认
   - 按查询成本分层选择防护方式

2. **Redis 挂了怎么办？**
   - 读路径：L2 超时后降级直查 MySQL
   - 写路径：缓存更新失败只打日志，不影响业务
   - 限流器：直接放行
   - 分片上传：返回 503（唯一例外）

3. **点赞的最终一致性如何保证？**
   - API 只等 MQ 发布确认
   - Worker 幂等消费（唯一索引 + RowsAffected）
   - MQ 故障时降级事务直写
   - 重复消费不会导致重复计数

4. **热榜翻页为什么用 as_of？**
   - 热度实时变化，不用快照会导致翻页跳页/重复
   - as_of 冻结时间窗口，同一窗口内翻页结果一致
   - 代价是快照 2 分钟后过期

5. **为什么关注不走异步落库？**
   - 关注是强一致需求，用户关注后期望立即生效
   - MQ 仅用于通知（如推送），不承载落库
