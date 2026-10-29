# Phase 2：数据模型（10 张表）

> 学习目标：搞清楚每张表存什么业务数据、索引为什么这么设计、字段定义为什么按模块分散。

## 0. 先校准三个事实

读代码前先纠正容易带偏的预期：

1. **实际落库 10 张表**，见 `backend/internal/db/db.go` 的 `AutoMigrate` 清单：
   `accounts`、`videos`、`likes`、`comments`、`socials`、`outbox_msgs`、`tags`、`video_tags`、`messages`、`notifications`。
2. **`feed/entity.go` 和 `video/chunk_entity.go` 不是表**：
   - `feed/entity.go` 全是 DTO（请求/响应结构体），没有 gorm 标签，不落库。
   - `ChunkUploadSession` 是分片上传的会话状态，存 Redis，不进 MySQL。
3. **`Comment` 表没有 `parent_id`**：当前是扁平评论，不支持楼中楼。

判断一个结构体是不是真表的方法：**有 gorm 标签 + 被 `AutoMigrate` 注册**，两条都满足才算。

存储位置速查：

| 结构体 | 存储位置 |
|---|---|
| 10 张表对应的结构体 | MySQL（GORM AutoMigrate 建表） |
| `ChunkUploadSession` | Redis（带过期时间的临时状态） |
| `feed/entity.go` 全部 | 哪都不存，纯接口 DTO |

## 1. 领域分组总览

```
Account 域: accounts
Video 域:   videos(核心) / likes / comments / tags / video_tags
Social 域:  socials
Message 域: messages / notifications
Infra 域:   outbox_msgs
```

`videos` 是唯一的核心实体，其余表要么挂在它身上（likes/comments/tags），要么挂在 accounts 身上（socials/messages/notifications），`outbox_msgs` 是纯基础设施。

## 2. 逐表详解

### 2.1 accounts —— 用户账户表

文件：`backend/internal/account/entity.go`

| 字段 | 存的业务数据 |
|---|---|
| `id` | 用户唯一 ID |
| `username` | 用户名，`gorm:"unique"` 唯一索引，登录和注册都按它查 |
| `password` | 密码，`json:"-"` 序列化时丢弃，任何接口都不会泄到前端 |
| `token` / `refresh_token` | 登录态令牌，同样 `json:"-"`；`token` 存 access 原值，`refresh_token` 存 **SHA-256**，另配 `prev_refresh_token`（上一张哈希）与 `refresh_rotated_at`（轮换时间） |
| `refresh_token_expiry` | refresh 的绝对过期时间，Redis 全挂时由它兜底 |
| `avatar_url` / `bio` | 头像地址、个人简介 |

业务场景：注册、登录、改密码、个人主页。

### 2.2 videos —— 视频内容表（核心表）

文件：`backend/internal/video/video_entity.go`

视频文件本身存 OSS/文件存储，表里只存元数据和地址。

| 字段 | 存的业务数据 |
|---|---|
| `id` | 视频唯一 ID |
| `author_id` | 发布者账号 ID，关联 `accounts.id`，有普通索引 |
| `username` | 发布者用户名（**冗余存储**，避免每次查询都连表） |
| `title` / `description` | 标题、描述 |
| `play_url` / `cover_url` | 播放地址、封面图地址 |
| `create_time` | 发布时间，服务"最新视频流"排序 |
| `likes_count` | 点赞总数，**同步更新**，用于展示点赞数与写热度事件 |
| `popularity` | 热度分，**异步更新**，服务"热门榜" |

### 2.3 likes —— 点赞记录表

文件：`backend/internal/video/like_entity.go`

一条记录 = 一次有效点赞（谁赞了哪个视频）。

```go
VideoID   uint `gorm:"uniqueIndex:idx_like_video_account;not null"`
AccountID uint `gorm:"uniqueIndex:idx_like_video_account;not null"`
```

两个字段共享同一个 `uniqueIndex` 名字 → GORM 合成**联合唯一索引 `(video_id, account_id)`**。语义：同一用户对同一视频只能点赞一次。这是**数据库层的幂等兜底**——即使上层代码出 bug 或并发重复请求，DB 直接拒绝第二条插入。

### 2.4 comments —— 评论表

文件：`backend/internal/video/comment_entity.go`

| 字段 | 说明 |
|---|---|
| `video_id` / `author_id` / `username` | 评论挂在哪个视频下、谁发的（均有索引） |
| `content` | 评论正文 |

扁平结构，**没有 parent_id**，不支持回复链。

### 2.5 tags + video_tags —— 标签多对多

文件：`backend/internal/video/tag_entity.go`

经典多对多拆解：
- `tags`：存去重后的标签名，`name` 上有 `uniqueIndex`。
- `video_tags`：纯关联表，`video_id` 和 `tag_id` 各一个普通索引，支持双向查询（"这视频有哪些标签" / "这标签下有哪些视频"）。

同文件里的 `ExtractTags` 用正则 `#([\p{L}\p{N}_]+)` 从标题/描述里提取 `#话题`。

### 2.6 socials —— 关注关系表

文件：`backend/internal/social/entity.go`

一张表挂**三个索引**，各司其职：

```go
FollowerID uint `gorm:"not null;index:idx_social_follower;uniqueIndex:idx_social_follower_vlogger"`
VloggerID  uint `gorm:"not null;index:idx_social_vlogger;uniqueIndex:idx_social_follower_vlogger"`
```

- `idx_social_follower_vlogger`（联合唯一）：防重复关注，同 likes 表的幂等兜底思路。
- `idx_social_follower`：查"我关注了谁"（`WHERE follower_id = ?`）。
- `idx_social_vlogger`：查"谁关注了我"（`WHERE vlogger_id = ?`）。

两个查询方向都要快，所以两个单列索引都不能省。

### 2.7 messages —— 私信表

文件：`backend/internal/message/entity.go`

用户一对一聊天记录。`from_id` 和 `to_id` 各有独立索引——私信要分别支持"我发出的"和"发给我的"两个方向的查询。`is_read` 支持未读提醒。

### 2.8 notifications —— 系统通知表

文件：`backend/internal/worker/notificationworker.go`（注意：不在独立的 entity 文件里）

点赞/关注/评论后由 Worker 异步生成的用户提醒。

| 字段 | 说明 |
|---|---|
| `recipient_id` | 接收者，**有索引**——通知只按"我的通知列表"查 |
| `sender_id` | 触发者（谁赞了你），**无索引**——从不按发件人查 |
| `type` / `target_id` / `content` | 通知类型、关联对象、文案 |
| `is_read` | 是否已读 |

**索引只为真实存在的查询而建**，这是判断该不该加索引的第一原则。

### 2.9 outbox_msgs —— 本地消息表（基础设施）

文件：`backend/internal/video/video_entity.go`（L42-L48）

Event-driven 的关键，不是业务表。

| 字段 | 作用 |
|---|---|
| `video_id` | 关联业务实体，有索引 |
| `event_type` | 事件类型，如 video_published |
| `status` | 投递状态，有索引——Worker 轮询 `WHERE status='pending'` 捞未投递事件 |

**Outbox 模式**：业务写入和 outbox 写入放同一个本地事务，保证"视频一定落库 + 事件一定落表"；Worker 再异步把事件搬到 RabbitMQ。即使 MQ 挂了事件也不丢——这就是 API 进程对 RabbitMQ 降级设计的另一半：发不出去就先存表里。

## 3. 索引为什么这么设计

`videos` 表的 3 个索引，**每一个对应 feed 模块的一条真实排序查询**（见 `backend/internal/feed/repo.go`）：

| 索引 | 列 | 服务的查询 |
|---|---|---|
| `index`（author_id 单列） | `author_id` | 个人主页"TA 的作品"列表 |
| `idx_videos_create_time` | `(create_time DESC)` | 最新视频流 `ORDER BY create_time DESC` |
| `idx_videos_popularity_time_id` | `(popularity DESC, create_time DESC)` | 热门榜 DB 兜底：同序 keyset 分页 |

核心规律：**复合索引的列顺序 = ORDER BY 的列顺序**。热门榜的 keyset 条件：

```sql
(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)
```

索引 `(popularity DESC, create_time DESC)` 让 MySQL 按索引顺序直接扫描，前两层比较都命中索引。

已知小瑕疵：索引名里有 `_id`，但定义里**并没有把 `id` 列加进去**，keyset 第三层 `id < ?` 不在索引内，极端同分情况下要回表排序。不影响正确性，只影响同分数据多时的性能。

## 4. Popularity 和 likes_count 的区别

两者都是 `videos` 表上的计数器列，但**语义、写入路径、服务的查询完全不同**：

| | `likes_count` | `popularity` |
|---|---|---|
| 语义 | 纯点赞数 | 综合热度分（点赞 +1、评论 +1 都加分） |
| 写入方式 | **同步**，点赞事务里 `likes_count + 1`；取消用 `GREATEST(likes_count - 1, 0)` 防负数（见 `like_service.go`） | **异步**，PopularityWorker 消费 MQ 事件后更新 |
| 存储 | 只有 MySQL 一列 | **双层**：Redis 分钟窗口 ZSET（`hot:video:1m:{yyyyMMddHHmm}`，见 `popularity_cache.go`）+ MySQL 列兜底 |
| 服务的榜 | 无独立榜单，仅展示点赞数 | 热门榜（读 Redis 窗口合并快照，Redis 挂了降级到 MySQL） |

为什么这样拆：`likes_count` 是**事实数据**，必须精确、强一致，走同步事务；`popularity` 是**派生的排序信号**，允许秒级延迟，走异步事件 + Redis 窗口——热榜读 QPS 最高，用 Redis 扛住，MySQL 只做降级兜底。这正是"同步链路 vs 异步链路"分离在数据模型上的体现。

## 5. 字段定义为什么按模块分散，而不是放一个 models 包

先纠正前提：**建表管理是集中的**（`AutoMigrate` 一个函数注册全部 10 张表），分散的只是字段定义。这是刻意的：**定义跟着业务走，注册集中在一点**。

分散封装的 4 个原因，按重要性排：

1. **高内聚**：`Video` 结构体和它的 repo/service/handler 都在 `internal/video/` 里，改字段只动一个目录。合并后 models 包会变成全项目改动最频繁的文件、git 冲突集中地。
2. **控制依赖方向，防循环依赖**：social 包直接复用 account 包的类型（`social/entity.go` 里 `import "feedsystem_video_go/internal/account"`），依赖是单向链 social → account。全部塞一个包就变成谁都能改的公共抽屉。
3. **Go 社区惯例**：package 按功能域划分（modular monolith），反对 `models/` + `controllers/` + `services/` 的技术分层。标准库自己就这么做。
4. **包名就是命名空间**：`video.Like` 一看就知道归属，`models.Like` 还得猜。

一句话：**字段定义分散是为了改动局部化，建表注册集中是为了管理统一化**，两者不矛盾。

## 6. 用户操作 → 写哪些表

| 用户操作 | 写入/更新的表 |
|---|---|
| 注册/登录 | `accounts` |
| 发布视频 | `videos` + `outbox_msgs` + `tags` + `video_tags` |
| 点赞 | `likes` + 更新 `videos.likes_count` + 写热度事件 |
| 评论 | `comments` + 异步更新 `videos.popularity` |
| 关注/取关 | `socials` + 异步写 `notifications` |
| 发私信 | `messages` |
| 刷热门榜 | 读 Redis 热度窗口（主）/ `videos`（兜底） |
| 刷最新视频 | 读 `videos` 按 `create_time` 排序 |
| 查看消息中心 | 读 `notifications`、`messages` |

## 7. 自查清单

学完应该能回答：

- [ ] 10 张表分别是什么？哪些 entity 文件不是表？
- [ ] `likes` 和 `socials` 的联合唯一索引各防什么？
- [ ] `videos` 表 3 个索引分别服务哪条查询？
- [ ] `popularity` 和 `likes_count` 在语义、写入路径、存储层上的三点区别？
- [ ] `outbox_msgs` 解决什么问题？为什么它和业务写入要在同一事务？
- [ ] 为什么 `notifications.sender_id` 没有索引？
