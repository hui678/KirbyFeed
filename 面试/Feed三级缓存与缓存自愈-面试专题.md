# 面试专题：Feed 三级缓存与缓存自愈

> 代码位置：`backend/internal/feed/service.go`、`backend/internal/video/video_cache.go`
> 配套源码级笔记：`start-learn/缓存体系.md`（本专题是它的面试版：先用这篇建立链路，再用那份抠细节）
> 配套问题清单：`面试/已知问题清单（面试弹药库）.md`（修了什么、还剩什么、面试怎么讲）
>
> **行号基准：2026-10-24**，即「视频实体 key / TTL 收敛」+「关注流 context 修复」+「并联双机制收敛」之后的代码状态。
> 使用方式：**先读 §0.5 定「推 / 拉」的调子** → 通读第 1~6 节 → 背第 7 节话术 → 用第 8 节短板清单挑刺 → 第 10 节题库自测

---

## ⚠️ 读前先确认：哪些「detail」已经不存在了

文档里凡是提到 `video:detail` 或详情页的互斥锁，**一律是历史状态（改造前 / 修复前）**，不要当成现状背。

| 名称 | 现状 | 依据 |
|---|---|---|
| `video:detail:id=<id>` 这个 **Redis key** | ❌ **已删除**，并入 `video:entity:<id>` | 全库已搜不到该字符串 |
| `GetDetail` 里的 **SETNX 互斥锁** | ❌ **已删除**，改用共享 singleflight | `video_service.go:142` |
| `GetDetail` 这个**函数** | ✅ **还在正常运行** | `video_service.go:107` |
| `/video/getDetail` 这个 **API** | ✅ **还在挂牌** | `router.go:74` |
| `GetDetailRequest` | ✅ 还在 | `video_entity.go:33` |

> **一句话记法**：删掉的是「**detail 那份缓存副本**」和「**detail 那把锁**」，**不是「详情接口」本身**。
> 现在详情页与 Feed 列表共用**同一个 key**、**同一个 TTL**、**同一个 singleflight Group**。

---

## 0. 简历原话拆解（每一句都要能展开）

> **Feed 三级缓存与缓存自愈**：设计「本地内存（秒级 TTL）→ Redis（实体缓存 1h / 列表结果缓存 24h）→ MySQL」三级读取链路，Redis 层 MGet 批量回源；实体缓存用 singleflight 做进程内并发去重，列表结果缓存用 Redis SETNX 互斥锁做跨实例防击穿，按查询成本分层防护；写路径同步失效关联缓存（热度更新删视频实体缓存、关注变更删关注流缓存）；Redis 不可用或超时全量降级直查 MySQL，恢复后由请求异步回填实现缓存自愈。热点数据请求不再穿透到 MySQL。

拆成 **6 个可独立成题的考点**：

| # | 考点 | 一句话 |
|---|---|---|
| 1 | 三级链路 | L1 本地 → L2 Redis → L3 MySQL，逐层下沉，命中即返回 |
| 2 | 批量与超时 | L2 用 `MGet` 一次拿完，50ms 硬超时 |
| 3 | 分层防击穿 | 廉价查询用 singleflight（进程内），昂贵查询用 SETNX（跨实例） |
| 4 | 写路径失效 | 热度/删除 `DEL` 实体缓存，关注变更按 pattern 删关注流缓存 |
| 5 | 降级 | Redis 挂 / 超时 → 整体沉到 MySQL，不报错 |
| 6 | 自愈 | Redis 恢复后，靠正常请求的异步回写把缓存重新填满 |

**听到这段话，面试官一定会追问**：为什么 L1 只有几秒？为什么要用两种防击穿手段？降级后 DB 扛得住吗？自愈是主动任务还是被动触发？——下面逐个答。

---

## 0.5 上位结论：Feed 推拉模式判定（先定调，再讲缓存）

> 这一节是第 1 节的**前置**：先把「这个项目是推还是拉」定死，再讲三级缓存为什么长这样。
> §1.1、§10 Q9 都会回到这里——那两处对「推模式」的旧表述已按本节口径修正。

### 0.5.1 一句话结论

> **本项目不存在推模式。** 最新 / 关注 / 热点三条流**全是拉模式（读扩散）**，区别只在「读的时候从哪拿候选 ID」。
> 没有 per-user 收件箱、没有写扩散，发布一条视频的写放大恒为 **O(1)**，大 V 发视频零扇出。

### 0.5.2 判据：别看「写侧有没有预置数据」，要看 fanout 发生在写还是读

| | 推模式（写扩散 fanout-on-write） | 拉模式（读扩散 fanout-on-read） |
|---|---|---|
| 发布时 | 把内容 ID 扇出到**每个粉丝各自的收件箱** | 只写**一份** |
| 典型 key | `feed:inbox:<userID>`（**每人一份**） | 无收件箱 |
| 写放大 | **O(粉丝数)** —— 大 V 发一条 = 百万次写 | **O(1)** |
| 读放大 | O(1)，直接读自己那份 | O(关注数)，读时按关系聚合 |
| 各自痛点 | 大 V 写扇出、僵尸粉空写 | 读时聚合慢 |

**唯一硬判据：有没有 per-user 收件箱。** 全库搜不到 `feed:inbox`、也搜不到任何带 `accountID` 的时间线结构 → 就没有写扩散。

本项目 `feed:` 开头的 key 只有三类，**没有一类是收件箱**：

| Key | 归属 | 维度 |
|---|---|---|
| `feed:global_timeline` | 最新流 | **全局一份**的物化索引 |
| `feed:listByFollowing:limit=<n>:accountID=<id>:before=<u>` | 关注流 | 查询**结果**缓存（读时才生成，不是预写） |
| `hot:video:1m:<yyyyMMddHHmm>` / `hot:video:merge:1m:<as_of>` | 热榜 | **全局一份**的计数器 |

### 0.5.3 三条流逐条拆：写侧预置的到底是什么

| 流 | 写侧预置 | 读时才做的事 | 涉及粉丝关系？ | 结论 |
|---|---|---|---|---|
| **最新** `listLatest` | `feed:global_timeline` ZSET：`videoID → createTime`，**全局一份** | 按游标 `ZRevRangeByScore` 取一页 ID → hydrate → 冷尾回 MySQL 拼 | ❌ | **拉**（索引是物化视图） |
| **关注** `listByFollowing` | 无 | 子查询 `author_id IN (关注列表)` → 排序 → 取一页 | ✅ | **拉**（最原始形态） |
| **热点** `listByPopularity` | `hot:video:1m:<分钟>`：**热度分**累加 | `ZUNIONSTORE` 聚合 60 个窗口 → 快照 → 分页 → hydrate | ❌ | **拉**（写的是分数，不是内容） |

最新流读侧（第 239 行取 ID、第 252 行 hydrate）：

```239:239:backend/internal/feed/service.go
		videoIDsStr, err := f.rediscache.ZRevRangeByScore(ctx, f.rediscache.Key("feed:global_timeline"), maxScore, "-inf", 0, int64(limit))
```

热榜的写侧**只加分数，不分发内容**：

```19:26:backend/internal/video/popularity_cache.go
	now := time.Now().UTC().Truncate(time.Minute)
	windowKey := cache.Key("hot:video:1m:%s", now.Format("200601021504"))
	member := strconv.FormatUint(uint64(id), 10)

	// 每个 Redis 操作各自申请独立超时预算：带 timeout 的 context 的额度会被
	// 串联在上面的操作连续消耗，共用的话后一步会因为预算耗尽而静默失败。
	incrCtx, incrCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
	_ = cache.ZincrBy(incrCtx, windowKey, member, float64(change))
```

而**排序、聚合、分页全部发生在读请求进来之后**（第 479-490 行：建 80ms 预算 → `Exists` → `ZUnionStore` 合并 60 个窗口 → `ZRevRange` 切片）。

### 0.5.4 ⚠️ 纠偏：冷热分离 ≠ 推模式（最容易被追问打穿的地方）

`listLatest` 的时间线确实是**发布时**异步写进去的（Outbox → `video.timeline.events` → ZSET），看起来"有推的成分"。但：

1. 它推的是**全局一份的索引**，不是「给每个粉丝各准备一份」；
2. 粉丝关系**完全不参与**，大 V 发视频零扇出；
3. 读请求进来才决定「这一页给谁、取哪一段、怎么 hydrate」。

> **一句话**：预写一份全局物化索引 = **加速读的优化**，不是**写扩散**。
> 判据永远是 **O(1) 还是 O(粉丝数)**，不是「写在前面还是后面」。

**顺带纠正一个内部叫法**：严格说只有 `listLatest` 是冷热分离（热 ZSET / 冷 MySQL 拼接）。热榜那套更准确叫**滑动窗口计数 + 读时快照聚合**——它的 MySQL 兜底（`ListByPopularity` 复合游标）是 **Redis 故障时的降级路径**，不是冷尾数据。

### 0.5.5 用户获取视频的全量入口（不止「三条流」）

| 分组 | 接口 | 场景 | 前端 | 模式 |
|---|---|---|---|---|
| Feed | `/feed/listLatest` | 首页「最新」 | ✅ | 拉 + 冷热分离 |
| Feed | `/feed/listByPopularity` | 「热门」 | ✅ | 拉 + 计数聚合 |
| Feed | `/feed/listLikesCount` | 「点赞最多」 | ✅ | 拉（复合游标） |
| Feed | `/feed/listByFollowing` | 「关注」 | ✅（需 JWT） | 拉 |
| Feed | `/feed/listByTag` | 话题流 | ❌ **后端有，前端未接** | 拉 |
| Video | `/video/listByAuthorID` | 他人主页作品 | ✅ | 拉 |
| Video | `/video/getDetail` | 详情 / 播放页 | ✅ | 拉 + 实体缓存 |
| Like | `/like/listMyLikedVideos` | 我的点赞 | ✅ | 拉 |

```174:186:backend/internal/http/router.go
	feedGroup := r.Group("/feed")
	feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))
	{
		feedGroup.POST("/listLatest", feedHandler.ListLatest)
		feedGroup.POST("/listLikesCount", feedHandler.ListLikesCount)
		feedGroup.POST("/listByPopularity", feedHandler.ListByPopularity)
		feedGroup.POST("/listByTag", feedHandler.ListByTag)
	}
	protectedFeedGroup := feedGroup.Group("")
	protectedFeedGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedFeedGroup.POST("/listByFollowing", feedHandler.ListByFollowing)
	}
```

**半入口**（不算列表，但把用户导到视频）：

- 通知 SSE：事件里带 `video_id`，前端点跳转 `/video/:id` → 最终落到 `getDetail`；
- `/static`：`r.Static("/static", "./.run/uploads")` 直出视频与封面，是播放链路不是列表；
- `/account/getProfile`：只返回 `video_count / total_likes` 聚合数字，**不含视频列表**。

> **可主动抛的短板**：项目**没有搜索接口**，按标题 / 描述关键词检索做不到，目前只有一个内容检索维度 —— 发布时从标题描述里提取的 `#话题`，而且 `listByTag` 前端还没接。

### 0.5.6 ⭐ 追问：什么时候你才应该改成推模式？

> 当**关注流的读放大扛不住**时（用户平均关注数高 + 活跃作者多，每次读都要 JOIN 关注表再排序）。
> 改法：给每个用户建 `feed:inbox:<userID>` ZSET，发布时异步扇出到粉丝收件箱，读时直接 `ZREVRANGE` 自己那份，读放大 O(关注数) → O(1)。
> **但大 V 不能扇出** —— 这就是微博的**推拉结合**：普通作者发布走推（扇出到粉丝收件箱），大 V 发布只写自己的发件箱 `feed:outbox:<authorID>`，读者拉取时**单独 merge 大 V 的发件箱**。这样把「写放大 O(粉丝数)」和「读放大 O(关注数)」同时压住。

**这个追问的价值**：能答出「大 V 是推拉结合的分界线」，说明你不是背概念，而是知道推模式的真实代价在哪。

### 0.5.7 面试话术（建议背）

> 「项目**不存在推模式**，三条流全是拉模式。区别只在读的时候从哪拿候选 ID：关注流读 MySQL 关系表，O(关注数)；最新流读 Redis 全局时间线 ZSET，冷热分离、冷尾回 MySQL；热榜读 Redis 分钟窗计数、读时 `ZUNIONSTORE` 聚合。
> 三者的写侧都是 O(1)，都不按粉丝扇出 —— **没有收件箱，就没有写扩散**。
> 我特意区分过一件事：最新流的时间线虽然是发布时写进去的，但那是一份全局物化索引，不是给每个粉丝准备一份，所以它仍然不是推模式。」

---

## 1. 三级缓存整体链路

### 1.1 定位（先纠正常见误解）

`FeedService.GetVideoByIDs`（第 44 行）输入一批 videoID，输出按原顺序排好的视频实体。

⚠️ **它全项目只有两个调用方**：第 252 行的 `ListLatest`、第 514 行的 `ListByPopularity`。这点一定要记牢：

| 接口 | 走 `GetVideoByIDs` 吗 | 为什么 |
|---|---|---|
| `ListLatest`（最新流） | ✅ | ZSET 只存 `{videoID → 时间戳}`，**只有 ID 没有内容**，必须再 hydrate 一次 |
| `ListByPopularity`（热榜） | ✅ | ZSET 快照只给 ID 顺序，内容同样要按 ID 装配（原本遗漏，已补齐） |
| `ListByFollowing`（关注流） | ❌ | MySQL 子查询直接 `SELECT` 出 `videos` 整行，内容已经有了 |

> **一句话**：只有「先拿 ID 列表、再按 ID 装配内容」的路径才需要实体缓存；SQL 直接吐整行的路径插不进去。
>
> ⚠️ **别把「ZSET 只存 ID」直接等同于推模式** —— ZSET 存 ID 只是逼你必须 hydrate，**这件事与推/拉无关**。本项目三条流**全是拉模式**（详见 §0.5），但最新流和热榜依然要 hydrate。
> 推 / 拉只取决于一件事：**有没有 per-user 收件箱、写放大是 O(1) 还是 O(粉丝数)**。

### 1.2 链路图

```
videoIDs[]（一页 20 个 ID）
   │
   ├─► L1  本地 go-cache（进程内存，实际 5s）
   │        命中 → 直接进 videoMap，不再往下走
   │        未命中 → 收集进 missedL1
   │
   ├─► L2  Redis MGet（一次批量，50ms 超时）
   │        命中 → 反序列化 → 回写 L1(5s) → 进 videoMap
   │        未命中 → 收集进 missedL2
   │        整体报错/超时 → missedL2 = missedL1（全量降级）
   │
   └─► L3  MySQL：每个 miss 的 ID 一个 goroutine
             └─ singleflight.Do("sf:entity:<id>")
                  ├─ leader：查库 → 异步回写 Redis(1h)
                  └─ follower：阻塞等待，共享结果
             → 各自写 L1(5s)
   │
   ▼
buildOrderedResult(videoIDs, videoMap)   ← 按原顺序重排（缓存回填顺序不可控）
```

### 1.3 易错点：L1 的 TTL 到底是 3 秒还是 5 秒

构造函数第 41 行是 `cache.New(3*time.Second, 5*time.Second)`，两个参数分别是「默认 TTL」和「后台清理 goroutine 的扫描间隔」。但两处写入都**显式传了 5 秒**（第 94、148 行），显式 TTL 优先于默认值，所以：

- **实际生效的 L1 TTL = 5 秒**
- 3 秒这个默认值在本模块根本没用上（没有调用 `SetDefault`）

主动说出这个不一致，是加分项 —— 说明你真读过代码，而不是背八股。

---

### 1.4 三类缓存的区分：索引 / 实体 / 列表

第 1~4 节讲的「三级」是**层级**（L1 本地 → L2 Redis → L3 MySQL）。还有一条**正交**的轴很容易漏 —— 缓存的**种类**：

| | **索引缓存** | **实体缓存** | **列表/整页缓存** |
|---|---|---|---|
| Redis 结构 | **ZSET** | String | String |
| key 举例 | `feed:global_timeline`、`hot:video:1m:<分钟>` | `video:entity:123` | `feed:listByFollowing:limit=20:accountID=42:before=0` |
| value | **ID 集合 + 排序分数** | 一条视频的完整内容 | 整页响应（ID + 顺序 + 内容 + 游标） |
| 存内容吗 | ❌ | ✅ | ✅ |
| 存顺序吗 | ✅ | ❌ | ✅ |
| 因人而异 | 否 | **否** | **是** |
| TTL | 2min（热度窗）/ 长期 | 1h | 24h |
| 失效方式 | ZAdd / 重建 | 视频变更时 `DEL` | 关注/取关时 `DelByPattern` |
| 用在哪 | 最新流、热榜、热度统计 | 最新流 + 热榜 + 详情页 | 关注流 |

**为什么叫「实体」**：它 1:1 对应数据库里的一条记录（有稳定标识 ID 的领域对象），按标识索引、**全局只有一份**、不因人而异。

> 顺带说个命名上的事：它原来叫 `video:detail:id=<id>`（「详情页的缓存」）—— 这个命名本身就埋了祸根，暗示它归详情页私有，所以详情页才给它配了独立 TTL 和独立锁。收敛成 `video:entity:<id>` 之后语义变成「**这个视频对象本身的缓存**」，三条路径共享才顺理成章（即 §5.1 那轮重构）。

**索引缓存是实体缓存存在的理由**：

```
ZSET 告诉你「哪些 ID、什么顺序」      ← 没有内容
实体缓存告诉你「这些 ID 分别是什么」   ← 没有顺序
两者拼起来 = 完整的一页
```

三条流的组装方式：

| 接口 | 怎么拼 |
|---|---|
| 最新流 `ListLatest` | 索引（ID + 顺序）+ 实体（内容） |
| 热榜 `ListByPopularity` | 索引（ID + 顺序）+ 实体（内容） |
| 关注流 `ListByFollowing` | **整页缓存一次搞定** —— 不需要拼，因为 SQL 直接出整行 |

⚠️ **实体缓存里没有的两样东西**：**顺序/排名**，以及**个性化字段**（是否点赞）—— 后者是 `buildFeedVideos(ctx, videos, viewerAccountID)` 后补的。正因为不含这两样，它才能被三个接口共享。

---

## 2. L1 本地缓存：为什么存在、为什么这么短

### 2.1 解决什么问题

吸收**同一实例上毫秒级的重复读**。一个热点视频在同一瞬间被几百个请求拉取，走 L1 就是纯内存哈希查找，零网络、零序列化开销。

### 2.2 为什么只有几秒（高频追问）

L1 是 `patrickmn/go-cache`，**纯进程内存，N 个 API 实例就有 N 份互不可见的副本**。

而写路径的缓存失效**只删 Redis**（见第 5 节），**没有任何跨实例广播去清别人的本地缓存**。

所以 **L1 的 TTL = 最坏情况下的脏数据窗口**：

- TTL 太长 → 实例 A 的 L1 感知不到实例 B 触发的 `DEL`，用户看到过期点赞数 / 已删除的视频；
- TTL 太短 → 失去本地缓存的意义。

**5 秒是折中**：Feed 对这几秒的旧数据不敏感，但足以把同一热点视频的瞬时并发收敛成本地内存读。

> 这里要能说出「代价」：关注/取关后，别的实例本地缓存仍可能在 5 秒内返回旧列表 —— **这是 L1 短 TTL 存在的原因，也是它无法消除的代价**。

### 2.3 三级共用同一套 key 构造

```55:55:backend/internal/feed/service.go
		cacheKey := video.VideoEntityKey(f.rediscache, id)
```

key 构造、TTL、失效都收敛在 `internal/video/video_cache.go`：

| 符号 | 作用 | 位置 |
|---|---|---|
| `VideoEntityTTL` | 实体缓存 TTL，常量 `1h` | `video_cache.go:13` |
| `VideoEntityKey(cache, id)` | 生成 `v1:video:entity:<id>` | `video_cache.go:16` |
| `InvalidateVideoEntity(ctx, cache, id)` | nil 安全的单一失效入口 | `video_cache.go:21` |

**好处**：L1 本地缓存、L2 Redis、详情页 `GetDetail` 三处共用同一个 key 构造，不存在两套命名规则。

---

## 3. L2 Redis：批量 + 硬超时 + 降级

### 3.1 三个设计点

```80:82:backend/internal/feed/service.go
		cacheCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		results, err := f.rediscache.MGet(cacheCtx, cacheKeys...)
		cancel()
```

1. **`MGet` 批量**：一页 20 个 ID 只用 1 次网络往返，而不是 20 次 `GET`；
2. **50ms 硬超时**（常量见第 25 行）：Redis 抖动不能拖垮整个请求的响应时间；
3. **错误不等于失败**：Redis 出错时不返回 error，而是把全部 miss 沉到 MySQL。

> **`singleRedisOpTimeout` 这个常量是踩坑后的产物**，务必理解它的注释：
> 带 timeout 的 context 的额度会被串联在其上的所有 IO 操作**连续消耗**，所以每个 Redis 操作必须各自新建 context，不能跨「加锁 → 查库 → 回填」这种多步骤流程复用。详见第 8 节「已修复」部分。

### 3.2 降级分支（这是「自愈」的起点）

```102:106:backend/internal/feed/service.go
		} else {
			// 如果 Redis 挂了或者超时了，全部降级到 L3
			missedL2 = missedL1
			log.Printf("L2 Redis MGet 失败，全部降级到 MySQL: %v", err)
		}
```

注意是**全量降级**而不是部分降级 —— 因为 `MGet` 是一次调用，要么整体成功要么整体失败，不存在「一半拿到一半没拿到」。

### 3.3 为什么 `MGet` 成功了还会有 nil？（易错点）

**因为在 Redis 里 `nil` 就是「key 不存在」的标准表示，它不是错误。**

```redis
MGET video:entity:1 video:entity:2 video:entity:3
1) "{\"ID\":1,...}"     ← 命中
2) (nil)               ← 不存在（不是报错！）
3) "{\"ID\":3,...}"     ← 命中
```

`GET` 一个不存在的 key 返回 nil；`MGet` 是「批量 GET」，把每个 key 的结果**逐个平铺**成数组，缺失的位置填 `nil`。**数组长度永远等于传入的 key 数量。**

> **`err == nil` 只表示「这条命令执行成功了」，不代表「所有 key 都命中了」。** 命中与否要看数组元素。

所以第 84 行 `if err == nil` 之后，还要在第 87 行逐个判断。落到 `missedL2` 的其实有**三种**情况：

| 情况 | 判断位置 | 性质 |
|---|---|---|
| `res == nil` | 第 87 行 | key 不存在 → 正常 miss |
| 不是 `string` 类型 | 第 88 行断言失败 | 类型不对 → 当 miss |
| JSON 解析失败 | 第 90 行 | 缓存内容损坏 → 当 miss |

**好处**：脏缓存不会因为 panic 或返回错误打断整个请求，而是被当作未命中重新回源 —— 这本身就是「自愈」的一部分。

### 3.3.1 那些 miss 掉的 key 接下来完整走什么路？

一页 20 个 ID，17 个命中、3 个是 nil：

```
MGet 成功，3 个 nil
   │
   ├─ 收进 missedL2（第 100 行）
   │
   ├─ 每个 ID 起一个 goroutine（第 116-151 行），并发查 MySQL
   │     └─ 每个都过 EntityLoadGroup 按 ID 去重（第 122 行）
   │
   ├─ 查到后回填两层：
   │     L1 本地（第 148 行，同步，5s）
   │     L2 Redis（第 131-139 行，异步 SetBytes，1h）
   │
   └─ buildOrderedResult（第 598 行）按入参原始顺序拼回
        └─ 不会因为这 3 个走了查库就乱序
```

**如果 MySQL 里也查不到**（视频真不存在）：第 125-127 行返回 `(nil, nil)` → `videoMap` 里没这条 → `buildOrderedResult` 跳过 → 结果里没有这个视频，而且**下次请求还会再查一次库**（即 8.2 的穿透问题）。

### 3.4 `MGet` 的取舍：1 次往返 vs 全有或全无

3.2 说「不能部分降级」，本质是 `MGet` **要么拿到完整数组、要么什么都拿不到**。这是它和「循环发 N 次 `GET`」的核心差异：

| | `MGet`（本项目） | N 次 `GET` |
|---|---|---|
| 往返次数 | **1 次** | N 次 |
| 一页 20 个 key 的耗时 | ≈ 0.3ms | ≈ 6ms（**20 倍**） |
| 部分失败时 | 影响整批 | 只影响失败的那几个 |

**为什么值得**：Redis 报错通常是「整体挂了 / 超时」这种**全局故障**，而不是「某几个 key 单独失败」。既然故障时基本都是全挂，用 1 次往返换掉「部分失败隔离」就划算 —— 省掉的是**每次请求都发生**的 20 倍 Redis 开销，付出去的是**罕见故障时才出现**的多查几个库。

⭐ **追问：那用 pipeline 批量 GET 不是两全其美？**
→ 能拿到逐命令的结果与错误，确实是折中方案。本项目没做，代价就是故障时全量降级。

---

## 4. L3 MySQL：singleflight 与异步回写

### 4.1 并发去重

```120:122:backend/internal/feed/service.go
			// 与详情页 GetDetail 共用同一个回源去重组：两条路径命中的是同一个
			// video:entity:<id>，只有共用 Group 与 key，并发回源才能真正合并。
			v, err, _ := video.EntityLoadGroup.Do(video.EntitySingleflightKey(videoID), func() (interface{}, error) {
```

`singleflight.Do(key, fn)`：同 key 的并发调用中，只有一个 goroutine 真正执行 `fn`（leader），其余**阻塞等待并共享同一份返回值**（follower）。

**100 个并发 miss 同一个 videoID → DB 只被查 1 次。**

本模块共 4 处 singleflight：

| key | 行号 | 防的场景 |
|---|---|---|
| `sf:entity:<videoID>` | 122 | 视频实体被并发读 —— **与详情页 `GetDetail` 共用 `video.EntityLoadGroup`**（见 8.1 F5）。注意它走的是 `EntitySingleflightKey()`，**不带 Redis 版本前缀**，因为这是纯内存 key |
| `sf:fallback:global_timeline_rebuild` **全局唯一** | 175 | ZSET 为空时，所有用户一起去重建 1000 条时间线 |
| `sf:cold:listLatest:<limit>:<reqTime>` | 223 | 冷尾分页（参数化） |
| `sf:stitch:listLatest:<remain>:<cursor>` | 270 | 冷热边界拼接（参数化） |

> 第 2 条是「全局静态锁」（重活必须无视用户差异合并），第 3、4 条是「参数化锁」（按用户/游标天然分散，用错粒度会把不同请求错误合并成一份结果）。

### 4.2 为什么必须 safeCopy（高频追问）

```129:129:backend/internal/feed/service.go
				safeCopy := *videoList[0]
```

```144:144:backend/internal/feed/service.go
				safeCopy := *(v.(*video.Video))
```

singleflight 把**同一个指针**返回给所有等待者（第 140 行 `return videoList[0]`）。如果直接把它塞进各自的 `videoMap`，任何一处修改都会污染其他请求的结果。所以拷贝两次：

| 拷贝 | 位置 | 解决什么 | 性质 |
|---|---|---|---|
| 第一次 | 第 129 行（闭包**内**） | 隔离「回源对象」与「Redis 序列化快照」 | **防御性**，当前非必需（`json.Marshal` 是同步执行的） |
| 第二次 | 第 144 行（闭包**外**） | 隔离「singleflight 共享返回值」与「每个并发请求各自的数据」 | **必需**，不拷贝就是并发数据污染 |

**为什么第二次是必需的**：等待者是**不同用户**的请求。一旦有人往这个共享对象上写个性化字段（比如 `IsLiked`），就会串到别的用户响应里。

同理第 148 行写 L1 时传的是 `safeCopy` 这个**值**而非指针 —— `go-cache` 存 `interface{}`，若塞指针进去，后续任何修改都会反向污染缓存。

> **诚实补充**：当前下游 `buildFeedVideos` 只读不写，所以严格讲第二次拷贝不写也跑得通。但这是把正确性建立在「后续没人改它」的约定上，拷贝成本几十字节，不值得省。

### 4.3 异步回写 Redis（自愈的执行者）

```131:139:backend/internal/feed/service.go
				if b, err := json.Marshal(safeCopy); err == nil {
					//异步回写redis
					go func(k string, b []byte) {
						setCtx, setCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
						defer setCancel()

						f.rediscache.SetBytes(setCtx, k, b, video.VideoEntityTTL)
					}(cachekey, b)
				}
```

两个原因：

1. **不让写缓存的耗时进入响应路径** —— 用户不等这个写；
2. **必须用 `context.Background()`** —— 请求结束后原 `ctx` 会被取消，复用它会导致回写必然失败。

TTL 是 **`video.VideoEntityTTL`（1 小时）**，对应简历里的「实体缓存 1h」。注意这个常量现在被 Feed 路径和详情页路径共用。

### 4.4 一个可以主动披露的取舍

每个 miss 的 ID **起一个 goroutine 单独查**（第 116-150 行），不是一个批量 `WHERE id IN (...)`。一页 20 个冷 ID 会产生 20 条单行 SQL。

**为什么这么做？** 为了配合 singleflight 的「按 ID 合并」粒度。如果批量查，key 就得是整批 ID 的组合，不同请求的 ID 集合稍有差异就无法合并。

**取舍是**：并发度高时能合并掉大量重复请求，但冷启动时会多出 N 次单行查询。要能说出「如果要改，怎么做」——比如未命中 ID 先批量查一次再分发。

---

## 5. 写路径：缓存怎么失效

| 数据 | 失效方式 | 代码位置 |
|---|---|---|
| `video:entity:<id>` | 热度/点赞变化时 `DEL` | `video/popularity_cache.go:17` |
| `video:entity:<id>` | 删除视频时 `DEL` | `video/video_service.go:95` |
| `feed:listByFollowing:*:accountID=<id>:*` | 关注/取关后按 pattern 删 | `social/service.go:94-102` |

```17:17:backend/internal/video/popularity_cache.go
	InvalidateVideoEntity(context.Background(), cache, id)
```

`DelByPattern` 用的是 **`SCAN` 迭代 + 逐个 `DEL`**，不是 `KEYS` —— 避免大 key 空间下阻塞 Redis 单线程。

### 5.1 一次重构：消灭视频实体的双份副本

**这是个可以讲的正面故事。**

改造前（**该状态已不存在，仅作复盘素材**）`videos` 表的同一行被缓存成两个 key：

| | `video:entity:<id>` | ~~`video:detail:id=<id>`~~（已废弃） |
|---|---|---|
| 服务 | `FeedService` | `VideoService` |
| 接口 | Feed 列表渲染 | `/video/getDetail` |
| TTL | 1h | 5min |
| 防击穿 | singleflight | SETNX（已删除） |

两者 `json.Marshal` 的产物**逐字段完全相同**（都是 `video.Video`，`video_entity.go:5-16`）。冗余带来的实际代价：

1. 同一份数据占两份 Redis 内存；
2. 失效必须成对删，漏一个就出现「Feed 显示 101 赞、详情显示 100 赞」；
3. **而且真的漏了**：`Delete` 原先只删 `video:detail`，导致**删掉的视频还能从 Feed 刷出来最长 1 小时**。

**修法**：抽出 `video_cache.go` 统一 key / TTL / 失效，`NewVideoService` 直接用 `VideoEntityTTL`（`video_service.go:26`），`Delete` 补上 `InvalidateVideoEntity`。

> 面试官问「有没有做过重构」——讲这个。

---

## 6. 防击穿：为什么用两套方案（核心考点）

这是整份简历里**最容易被追问**的一条。

> ⚠️ **先纠正措辞**：不是「同一份数据上用两套方案」—— 那是 F5 已经修掉的**并联** bug。
> 现在的两套是**分工**：视频实体用 singleflight、关注流列表用 SETNX，**每个 key 只有一套机制**。
> 判据是「**同 key 的并发堆积在哪**」：堆在同一个进程内 → singleflight（免费就能合并）；稀疏散落在不同 Pod → SETNX（进程内管不到，得靠全局锁）。

### 6.1 对比表

| | singleflight | Redis SETNX 互斥锁 |
|---|---|---|
| 作用域 | **单进程内**（内存 map） | **跨进程 / 跨实例** |
| 每次成本 | 一次内存操作，无网络 | 每个请求 1~N 次 Redis 往返 |
| 合并结果 | N → **P**（每 Pod 各 1 次回源） | N → **1**（理想，轮询超时者会漏） |
| 失败者行为 | 阻塞等待 leader，**拿到同一份返回值** | 轮询缓存，等不到就**自己查库兜底** |
| follower 延迟 | ≈ leader 查库耗时 | 轮询粒度起跳，上限 100ms |
| Redis 挂了 | **仍然有效** | 完全失效 |
| 本项目用于 | **视频实体读**：Feed 列表 `GetVideoByIDs` + 详情页 `GetDetail`（**两者共用同一个 Group**） | 关注流列表（重、体量大、缓存 24h） |

### 6.2 为什么「更弱的」singleflight 反而用得更多（必考题）

直觉是「SETNX 能全局合并，更强，为什么不全用它」。**因为合并不是免费的。**

设 `N` = 并发请求数、`P` = Pod 数、`C` = 单次回源成本、`R` = 一次 Redis 往返：

```
用 SETNX 多买到的收益 = 省掉 (P-1) 次回源 = (P-1) × C
用 SETNX 多付出的代价 = N × k 次 Redis 往返 + 每个 follower 最多 100ms 轮询
```

因为 `N >> P`，右边天然很大 —— **除非 C 也很大，否则这笔买卖永远亏。**

**算两个数字**（P=10，N=1000，R=0.3ms）：

| 场景 | 省下的 | 付出的 | 结论 |
|---|---|---|---|
| 视频详情/实体，C≈0.1ms | 9×0.1 = **0.9ms** DB | 1000×2 次往返 ≈ **600ms** Redis | **亏到离谱** |
| 关注流 JOIN，C≈50ms | 9×50 = **450ms** DB | 600ms Redis（但 Redis 比 MySQL 廉价） | **划算** |

**更致命的是延迟**：为了保护一个 0.1ms 的查询，让每个等待者多等 20~100ms —— 多等了 **200~1000 倍**。

**第三个理由（本项目特有）**：Redis 宕机降级时，singleflight 是**唯一还活着的防线**。热点路径恰恰必须保住。

→ **三元判断**：`回源成本 C` + `并发量 N` + `是否依赖 Redis`。三者都指向 singleflight 就用 singleflight。

### 6.3 SETNX 现场（ListByFollowing）

第 346-462 行，修复后的形态：

```404:406:backend/internal/feed/service.go
		readCtx, readCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		b, err := f.rediscache.GetBytes(readCtx, cacheKey)
		readCancel()
```

```413:422:backend/internal/feed/service.go
		} else if rediscache.IsMiss(err) { // 只有 key 明确不存在才回源，连接错误直接兜底查库
			lockKey := "lock:" + cacheKey

			lockCtx, lockCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			token, locked, lockErr := f.rediscache.Lock(lockCtx, lockKey, 500*time.Millisecond)
			lockCancel()

			if lockErr == nil && locked {
				// 解锁不能受请求生命周期限制，否则客户端断开会导致锁残留
				defer func() { _ = f.rediscache.Unlock(context.Background(), lockKey, token) }()

				if cached, ok := getCached(); ok { // 双重检查：等锁期间可能已被回填
					return cached, nil
				}
```

五个设计点：

1. **只在 `IsMiss` 时加锁**。`IsMiss` 等价于 `err == redis.Nil`。连接错误说明缓存层本身不可用，加锁无意义，直接兜底查库；
2. **双重检查**：拿到锁后立刻再读一次缓存 —— 从「发现 miss」到「拿到锁」之间，别人可能已经填好了；
3. **等待方优先**：没抢到锁的轮询 5 次 × 20ms 等 leader 回填，且用 `select` 响应 `ctx.Done()`；等不到才兜底查库。**宁可多一次 DB 查询，也不让请求无限阻塞**；
4. **每步独立超时预算**：`getCached` / `setCached` 各自新建 context（第 380-402 行），避免预算被前面的操作吃掉；
5. **缓存 24 小时**，靠关注/取关时的主动失效保证一致性。

### 6.4 锁的实现与「为什么不能直接 DEL」

```78:78:backend/internal/middleware/redis/redis.go
	ok, err = c.rdb.SetNX(ctx, key, token, ttl).Result()
```

`SetNX` = `SET key value NX PX ttl`，一条命令原子完成「不存在才写 + 带过期」，不存在先查再写的竞态。

解锁用 Lua 脚本比对 token：

```82:88:backend/internal/middleware/redis/redis.go
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)
```

**经典误删场景**（必须能讲出来）：

1. A 拿到锁，业务执行超过 500ms；
2. 锁自动过期，B 拿到锁；
3. A 执行完直接 `DEL` → 把 **B 的锁**删了。

token 让解锁变成「只删自己的锁」。`GET` 和 `DEL` 必须在**同一个 Lua 脚本**里，否则两步之间仍可能被插入。

### 6.5 两个具体例子：把 N→P 和 N→1 各走一遍

对比表（6.1）和账目（6.2）都偏抽象，面试时**举不出具体例子等于没真懂**。下面两个都是本项目的真实场景。

#### 例 1：singleflight —— 视频实体缓存

**场景**：热门视频 `videoID=123` 缓存失效，同一个 Pod 上同一瞬间有 **100 个请求**都在渲染包含它的页面。

```
没有 singleflight：100 个请求各自 miss → 100 次 SELECT * FROM videos WHERE id=123

有了 singleflight（第 122 行）：
  100 个请求全部调 Do("sf:entity:123", 查库函数)
      ├─ 第 1 个到达 → 成为 leader → 真的去查库
      └─ 后 99 个到达 → map 里已有该 key 在加载 → 阻塞在 Do 内部，什么都不做
  leader 返回 → 99 个等待者直接拿到同一个返回值，一起返回
```

**关键点**：leader 把返回值**直接交给** 99 个等待者，不走缓存、不走 Redis，纯内存传递。类比：100 个人围在前台问同一个问题，前台说「你们派一个进来问，我在门口告诉剩下 99 个」。

**为什么这里用它**：热点视频全站都在看，**同一个 Pod 内撞车概率极高**；而且它**免费**（就是个进程内存的 map）。

#### 例 2：SETNX —— 关注流列表缓存

**场景**：用户 42 刷关注流，缓存 `feed:listByFollowing:limit=20:accountID=42:before=0` 刚好过期。**20 个请求散落在 10 个 Pod 上**（连续上滑 / 刷新 / 客户端重试），每 Pod 约 2 个。

```
没有锁：10 个 Pod 各自回源 → 10 次昂贵的 JOIN（关注表 + 排序 + 分页）

有 SETNX（第 417 行）：
  20 个请求全部执行 SETNX lock:feed:listByFollowing:...:42 <token> PX 500
      ├─ Redis 单线程 → 只有 1 个返回 1（抢到）→ 双重检查 → 查库 → 回填 → 返回
      └─ 其余 19 个返回 0 → 第 437-446 行轮询 5×20ms → 读到缓存就返回
```

**结果：1 次 DB 查询。**

⚠️ **两个细节最容易答错**：

1. **每个 Pod 上的那 2 个不会被合并** —— 关注流这条路径**根本没有 singleflight**（只有 SETNX）。但它们也不会多查库：两个都没抢到锁，都走轮询，最终都从缓存读到。B 拿到结果**不是因为被合并**，而是**被锁挡住后轮询读到了缓存** —— 机制完全不同。
2. **赢的可能是任意一个 Pod 上的任意一个请求** —— 跟「哪个 Pod」无关，只跟「谁先执行到 SETNX」有关，因为锁在 Redis 里是**全局的**。

#### 为什么关注流不用 singleflight？（判据）

看 key 的构成（第 376 行）：`feed:listByFollowing:limit=%d:accountID=%d:before=%d`

**它带 accountID + limit + 游标 → 每个用户的每一页都不一样 → 同一个 key 的并发量天然极低**（一个用户通常一次就发一个关注流请求）。

| | 实体缓存 `video:entity:<id>` | 关注流 `feed:listByFollowing:...` |
|---|---|---|
| key 粒度 | **全站共享**（只含 videoID） | **每人每页各不相同** |
| 同 key 并发 | 极高（大家都在看同一个视频） | 极低（一个用户就一个请求） |
| 同 key 的并发堆在哪 | **大量堆积在同一个 Pod 内** | **稀疏地散落在不同 Pod 上** |
| singleflight 收益 | 大（进程内就能合并掉） | ≈ 0（进程内根本撞不上） |
| SETNX 收益 | 小（6.2 的账：不划算） | 大（能拦跨 Pod 散落的同 key） |

> **一句话**：列表缓存的并发形态是「**跨 Pod 稀疏**」—— 同 key 的请求散落在 10 个 Pod 上、每个 Pod 就一两个。singleflight 是进程内的，管不到别的 Pod；SETNX 是全局的，正好覆盖。

**所以不是「Pod 数据不一样导致不能合并」，而是「同 key 的并发根本不在同一个进程里」。** 判断能不能合并，看的是 **key 相不相同以及并发落在哪**，跟「在几个 Pod 上」不是一回事 —— 就算只有 1 个 Pod，100 个请求分属 100 个不同用户（100 个 key），singleflight 照样一个都合并不了。

#### 那「融合」（外层 singleflight + 内层 SETNX）在关注流上值不值？

用上面那个场景算：

| 方案 | SETNX 调用次数 | DB 查询次数 |
|---|---|---|
| 现在（只有 SETNX） | 20 次 | **1 次** |
| 加 singleflight（每 Pod 合并 2→1） | 10 次 | **1 次** |
| 差额 | 省 10 次 ≈ 3ms | **0** |

**DB 查询次数没变。** 融合省下的只有 Redis 往返，省不了数据库 —— 因为 SETNX 那把全局锁早就把回源收敛到 1 次了，singleflight 那层只能让「去抢锁的人」从 20 个减到 10 个。而正常流量下同 Pod 同 key 并发通常就是 1，收益接近于 0，所以本项目没做。

**什么时候才值得融合**：同一个 key **在单个 Pod 内并发很高**时（比如全站共用一个 key 的昂贵查询）。本项目没有这种场景。

---

## 7. 缓存自愈（本专题的核心）

### 7.1 定义

**自愈 = Redis 不可用期间服务不中断，Redis 恢复后缓存能自动重新填满，无需人工干预、无需额外的后台任务。**

### 7.2 三段式

| 阶段 | 发生了什么 | 代码 |
|---|---|---|
| ① Redis 故障 | `MGet` 报错/超时 → `missedL2 = missedL1` → 全部走 MySQL，请求正常返回 | 第 102-106 行 |
| ② 故障期间 | singleflight 按 ID 合并回源，同一视频最多打 1 次 DB；L1 仍提供 5 秒窗口；异步回写 Redis 静默失败 | 第 122、131-139 行 |
| ③ Redis 恢复 | 后续请求重新走到 L2 分支，miss 的 ID 照常回源并**异步回写**，缓存逐步填满 | 第 131-139 行 |

**关键点：自愈是「被动的、请求驱动的」**，没有定时任务、没有预热脚本。谁先访问谁负责重建。

### 7.3 面试话术（建议背下来）

> 「Redis 挂掉时我们不返回错误，而是把整批未命中 ID 直接沉到 MySQL —— 因为 `MGet` 是一次调用，所以是整体降级。这段时间靠 singleflight 按视频 ID 做进程内合并，保证同一个视频在同一时刻最多只有一次 DB 查询，同时 L1 的 5 秒本地缓存还在吸收重复读，所以 DB 的压力是收敛的而不是放大的。
> Redis 恢复后不需要任何运维动作：后续请求照常走三级链路，未命中的会回源并异步回写 Redis，缓存由**请求流量自然重建**。这就是自愈。」

### 7.4 一定要能接住的追问

**Q：降级后 DB 不会被打爆吗？**
A：三层缓冲：① L1 的 5 秒本地缓存；② singleflight 按 ID 去重（一页 20 个 ID 最多 20 次查询，而不是 20×并发数）；③ 上层还有 IP/账号维度的限流。
但要诚实补充：**没有熔断、没有限流兜底到 DB 这一层**，这是「可用性优先于 DB 保护」的取舍。

**Q：为什么不主动预热？**
A：请求驱动重建的粒度天然跟随真实热点，预热反而可能加载冷数据。代价是恢复瞬间有一波 DB 压力上升。

**Q：自愈期间的一致性？**
A：故障期间读到的是 MySQL 最新数据，恢复后回写的也是最新数据，所以自愈不会引入跨实例的旧数据污染。

---

## 8. 已修复 vs 已知短板

### 8.1 已修复（正面素材，优先讲）

**F4. 关注流的 context 超时预算被跨步骤复用** ⭐ 主推这条

- **位置**：`ListByFollowing`（修复前第 369-395 行）
- **现象**：一个 50ms 的 `cacheCtx` 被 `GET → SETNX → 双重检查 GET → SetBytes` **四步串起来共用**。DB 一慢 `SetBytes` 就超时失败，错误被 `_ =` 丢弃 → **缓存永远写不进去** → 等待方轮询永远落空 → 白等 100ms 后全部兜底查库 → **锁退化成串行查库**。
- **为什么难发现**：本地低负载时 DB 几毫秒返回，回填成功；**只有负载上去、DB 变慢才暴露**。
- **修法**：抽出 `singleRedisOpTimeout`（第 19-25 行），每步 Redis 操作各自独立申请预算；顺带补齐 `lockErr` 参与判断、轮询改成 `select` 响应取消。
- **面试讲法**：
  > 「我把一个 50ms 的 context 复用到了加锁和回填上。带 timeout 的 context 的额度是被串联在上面的操作连续消耗的，本地测不出来，DB 一慢回填就超时、错误还被吞了，结果缓存永远写不进去，锁实际退化成串行查库。原则是：**带 timeout 的 context 不能跨多个 IO 步骤复用**。」

**F5. 同一份数据的「并联双机制」收敛** ⭐ 很适合讲「架构复盘」

- **背景**：删掉冗余的 `video:detail` 键之后，`GetVideoByIDs`（singleflight）和 `GetDetail`（SETNX）开始命中**同一个** `video:entity:<id>`。
- **问题**：一条 key、两条回源路径，**各自一套防击穿机制**。两套机制**并联且互不感知** —— SETNX 的持锁者不知道还有 P 个 singleflight leader 正在查同一个视频，反之亦然。所以同一瞬间同一视频**仍有 P+1 次回源**，防护完全不叠加。
- **修法**：在 `video_cache.go` 建共享的 `EntityLoadGroup` + `EntitySingleflightKey`；详情页删掉 SETNX 改用同一个 Group（并补上 `safeCopy`）；Feed 侧从自己的 `requestGroup` 切到共享 Group。
- **收益**：① 一份数据一套机制；② 回源次数 P+1 → **P**；③ 意外的收获 —— **同一个视频的详情页请求和 Feed 列表请求现在会合并成一次查库**（这是修复前做不到的）。
- **面试讲法**：
  > 「删掉冗余 key 之后我才发现，两条读路径上各自有一套锁在保护同一份数据 —— 它们是并联的，彼此看不见，所以防护根本不叠加。我把回源收敛到一个共享的 singleflight Group，顺带让详情页请求和 Feed 列表请求能真正合并。」

**F6. 热榜漏走实体缓存** ⭐ 适合接在「三级缓存用在哪」之后讲

- **现象（修复前）**：`ListByPopularity` 直接 `repo.GetByIDs` 查库（原第 510 行），**没走三级缓存** —— 热门视频在最新流里已经缓存成 `video:entity:<id>` 了，热榜却还要再查一次库，等于同一份数据缓存了一份又查了一遍。
- **为什么算遗漏**：热榜的结构其实是「ZSET 快照给 ID 顺序 → 按 ID 装配内容」，和最新流**一模一样**，天然该走实体缓存。
- **修法**：第 514 行改用 `f.GetVideoByIDs(ctx, ids)`，删掉手工重排那段 —— `GetVideoByIDs` 内部的 `buildOrderedResult`（第 598 行）已按入参顺序返回并跳过缺失，语义等价。
- **现在 `GetVideoByIDs` 有两个调用方**：第 252 行 `ListLatest`、第 514 行 `ListByPopularity`。
- **为什么不用整页缓存**：① 翻页参数五个（`offset`/`asOf`/`latestPopularity`/`latestBefore`/`latestIDBefore`），组合爆炸命中率极低 ② ID 列表已被 ZSET 快照缓存过，再缓存整页是重复 ③ 内容装配是主键批量查询，本来就不贵。
- **面试讲法**：
  > 「热榜原来是漏的 —— 它直接查库，可热门视频在最新流里早就缓存成 `video:entity:<id>` 了。它的结构其实是『ZSET 快照给 ID 顺序、内容另外装配』，和最新流完全一样，所以直接换成走实体三级缓存就行。
  > 至于为什么不用整页缓存：热榜翻页参数有五个，组合爆炸命中率极低；而且 ID 列表已经被快照缓存过了，再缓存整页是重复。关注流才适合整页缓存 —— 它查询昂贵而且因人而异。**选型判据不一样。**」

**F7. 热度更新有两份实现，修了一处漏了另一处** ⭐ 讲的是「修复要顺着调用链查副本」

- **现象**：同一个动作「更新热度缓存」有**两份实现** —— `UpdatePopularityCache`（`popularity_cache.go`，被 `popularityworker.go:93`、`like_service.go:103/175`、`comment_service.go:87` 三处调用）和 `VideoService.UpdatePopularity` 里的**内联副本**。
- **后果**：在 `UpdatePopularityCache` 里把共用的 context 拆开之后，内联那份还留着 `50ms` 同时给 `ZincrBy` 和 `Expire` —— **以为修完了，其实只修了一半**。
- **根因**：同一逻辑两处维护，修复必然漏掉一份。
- **修法**：① `popularity_cache.go` 的 `ZincrBy` 与 `Expire` 各自申请独立预算（改用 `singleRedisOpTimeout`）；② 内联块改为直接调用 `UpdatePopularityCache`，重复实现删掉（顺带移除随之悬空的 `strconv` import）。
- **面试讲法**：
  > 「热度更新这个动作其实有两份实现：一份在 `popularity_cache.go` 里，被 worker、点赞、评论三处调用；另一份是 `VideoService.UpdatePopularity` 里的内联副本。我在第一份里把共用的 context 拆开了，但第二份还留着同样的错。
  > 这件事让我记住一个原则：**修 bug 要顺着调用链查有没有副本**，否则你以为修完了，其实只修了一半。」

> 💡 **F7 比 F4 更值钱**：F4 展示的是「能定位 bug」，F7 展示的是「**修完会回头查副本**」—— 这是初级和中级的分界线之一。

**F8. Redis 宕机时没有任何雪崩防护** ⭐⭐ 适合接在「缓存自愈」之后讲

先分清三个最常被混为一谈的概念：

| | **缓存击穿** | **缓存穿透** | **缓存雪崩** |
|---|---|---|---|
| 是什么 | 单个热点 key 失效，并发打穿 | 查询**根本不存在**的 key | 大量 key 同时失效 **或缓存层整体不可用** |
| 本项目 | singleflight / SETNX 已解决 | ✅ 已补（空值占位，见 F9） | ✅ 已补（`dbLoadGate` + 连接池） |
| 典型解法 | 互斥锁 / singleflight / 逻辑过期 | 空值占位 / 布隆过滤器 | TTL 抖动 / **熔断 / 限流** / 多级缓存 |

- **现象**：Redis 整体不可用时，`GetVideoByIDs` 走第 151-155 行「`MGet` 报错 → `missedL2 = missedL1`」全量降级；`ListByFollowing` 在 Redis 连接错误时也直接兜底查库（**SETNX 锁此时已失效**）。缓存层不再拦截任何流量，**singleflight 只能把 N 收敛到 P（Pod 数）**，压力毫无衰减地压到 MySQL。
- **为什么容易漏**：平时 Redis 好好的，这条路径根本走不到。而且 **TTL 随机抖动对这种雪崩完全无效** —— 它防的是成因 A（大量 key 同时过期），不是成因 B（缓存层不可用）。
- **修法**：补**两道闸门**，缺一不可
  - **驱动层**：`sqlDB.SetMaxOpenConns(64)` —— 原来 GORM 默认 `MaxOpenConns = 0`（**无上限**），雪崩时会一路建连接直到打满 MySQL 的 `max_connections`，那会让**所有**查询一起失败，连本来能成功的也跟着挂。这是谁都绕不过去的硬闸门
  - **应用层**：`dbLoadGate` 回源并发闸门，容量 `config.DefaultMaxConcurrentDBLoad = MaxOpenConns / 2 = 32` —— 只占一半，其余留给写路径、事务、迁移以及 social / video / like 等包
  - 应用层闸门**非阻塞**获取：拿不到配额 → 实体路径放弃本次回源（该条不出现在结果里）；关注流返回 `errDBOverloaded`
  - **不排队** —— 排队只是把堆积从数据库搬到应用内存，压力最终还是会落到库上
  - 用带缓冲 channel 而非 `sync.Semaphore`：`select` + `default` 一行就是「拿不到就降级」，语义最直白
- **过载为什么返回 503 而不是 500**：`errDBOverloaded` 包装 `apierror.ErrServiceOverloaded`，由 `ClassifyHTTPStatus` 映射为 503。这是**主动限流降级**而不是服务故障 —— 客户端收到 503 才知道该退避重试，收到 500 通常直接放弃。
- **面试讲法**：
  > 「Redis 全挂时我们是全量降级，这时候 singleflight 只能把 N 个请求收敛到 Pod 数，压力还是会压到库上 —— 这就是雪崩。我补了两道闸门：驱动层给连接池设上限（原来 GORM 默认无上限，会一路建连接把 MySQL 的 `max_connections` 打满，那会让**所有**查询一起失败，连本来能成功的也跟着挂）；应用层加回源并发闸门，用带缓冲 channel 当信号量，拿不到配额就直接放弃这次回源，**不排队**，因为排队只是把堆积从数据库搬到应用内存。
  > 降级表现分两种：实体路径是那条不返回、其余照常；关注流是整页的昂贵 JOIN，没法部分返回，所以回 503 让客户端退避。
  > 还有个点：TTL 随机抖动防的是『大量 key 同时过期』，对『Redis 整体挂掉』完全无效 —— 两种雪崩成因得分开看。」

> 💡 **这题的价值**：绝大多数候选人只会答「加 TTL 随机抖动」。能**区分两种雪崩成因**并指出抖动对其中一种无效，再给出熔断/限流的落地方式，是明显的高一档。

**F9. 缓存穿透：不存在的 ID 每次都打 MySQL** ⭐⭐ 必问题

- **现象（修复前）**：`GetVideoByIDs` 的 L3 闭包里

  ```go
  if err != nil || len(videoList) == 0 {
      return nil, err          // ← 什么都不记
  }
  ```

  查不到就返回，没有缓存「不存在」这个事实 → 同一个不存在的 videoID **每次请求都打一次 MySQL**。详情页 `GetDetail` 同理（`gorm.ErrRecordNotFound` 直接返回，也没记）。

- **为什么容易被忽略**：不存在的 ID 在自测里几乎不会出现 —— 你总是拿真实视频测。但它是**真实攻击面**：videoID 是自增 uint，可以轻易枚举。

- **修法：空值占位**（不是布隆过滤器）

  - 查不到时往 `video:entity:<id>` 写哨兵 `__null__`，TTL **60s**
  - L2 解码**在 `json.Unmarshal` 之前**识别哨兵 → `continue`，不进 `missedL2`，不回源
  - **两条读路径都接上**（`GetVideoByIDs` 和 `GetDetail`）—— 它们共用同一个 key，只改一边等于没改

- **三个必须知道的坑**（都是能答出「细节」的地方）：

  1. **哨兵不能用 JSON 的 `"null"`**：`json.Unmarshal([]byte("null"), &v)` **不报错**，只会把 `v` 留成零值 → 缓存出一个 `ID=0` 的垃圾实体。所以哨兵取**非法 JSON** `__null__`，且必须**在解析前显式识别**
  2. **只写不识别等于没做**：哨兵是非法 JSON，不先识别就会 `Unmarshal` 失败 → 掉进 `missedL2` → 照样查库
  3. **TTL 必须短**：60s。万一数据后来补上了，最多延迟这么久就能读到

- **为什么不用布隆过滤器**（必被追问）：

  | | 布隆过滤器 | 空值占位 |
  |---|---|---|
  | **新发布视频** | ❌ **误判为不存在 → 404**（BF 只保证「插入过的」查得到） | ✅ 该 ID 不在占位里，正常查库命中 |
  | **删视频** | ❌ 标准 BF 不支持删除，已删 ID 永久残留 → 误判率持续上升 | ✅ TTL 到期自动重查 |
  | **多实例** | ❌ 每个 Pod 一份内存 BF，发布要广播同步 | ✅ 都在 Redis，天然一致 |
  | **复杂度** | 预热任务 + 新依赖 + 误判率调参 | 几行 |

  而且「误判率设多少」没有真实流量只能拍脑袋 —— 跟熔断阈值是同一类问题。

- **代价（诚实说）**：恶意枚举攻击会让 Redis 多一批短 TTL 的占位 key。靠 60s TTL + Redis `maxmemory` 淘汰策略兜住 —— **真正防攻击该靠网关限流，不是缓存层**。

- **面试讲法**：
  > 「原来查不到什么都不记，同一个不存在的 ID 每次都打一次库 —— 这是穿透。我用空值占位：查不到就写一个 60 秒的哨兵，L2 读到哨兵直接跳过回源。
  > 没用布隆过滤器，因为它有个致命问题：**新发布的视频没同步进过滤器，会被误判成不存在、直接 404** —— 这是功能错误不是性能问题，而且演示时极难发现。再加上它不支持删除、多实例要各自同步、误判率还要调参，这个量级下不划算。
  > 还有个坑：哨兵不能用 JSON 的 `null`，因为 `json.Unmarshal("null", &v)` 不报错、会给你留一个零值实体出来。我取的 `__null__`，并且在解析前显式识别 —— 只写不识别等于没做。」

> 💡 **这题的价值**：绝大多数人只会背「布隆过滤器防穿透」。能说出**具体场景下为什么不用它**（新发布视频会被误判 404），再讲清哨兵为什么不能是 JSON 的 `null`，说明你是真写过、不是背的。

其余已修复项（实体双副本、删视频漏失效、TTL 不一致）见第 5.1 节。

### 8.2 已知短板（主动说出来 = 加分）

| 问题 | 位置 | 影响 | 改进 |
|---|---|---|---|
| **关注流整页缓存含完整元数据，视频变更无失效** ⭐ | 第 455-458 行 | 作者改标题/删视频后，别人的关注流里仍是旧数据，**脏到 24h TTL 才刷新** | 见下方专项说明（两套管法，权衡明确） |
| **`ListByPopularity` 的 `opCtx` 被 4 步共用** | 第 479 行 | `Exists` + `ZUnionStore`（合并 60 个 key，最贵）+ `Expire` + `ZRevRange` 共用 80ms | 每步独立 ctx，`ZUnionStore` 给更长预算 |
| **singleflight 用 `Do` 不用 `DoChan`** | 第 122/175/223/270 行 | `Do` 不接受 ctx，leader 卡死则**所有 follower 永久阻塞** | 换 `DoChan` + `select{ctx.Done(); time.After()}` + 超时兜底 |
| L1 默认 3s 与写入 5s 不一致 | 第 41、94、148 行 | 读代码时容易误判 TTL | 统一常量 |
| 冷启动按 ID 单个查询 | 第 116-150 行 | 一页 20 个冷 ID 产生 20 条单行 SQL | 未命中 ID 先批量查一次再分发 |
| 关注流等锁固定 5×20ms | 第 437-446 行 | 高负载下大量请求最终仍落到 DB | 指数退避 + 上限 |

---

**关注流整页缓存这段更要会讲** ⭐（第 455-458 行）

缓存里存的是**整个渲染结果**，不是「ID + 轻量字段」：

```10:20:backend/internal/feed/entity.go
type FeedVideoItem struct {
	ID          uint       `json:"id"`
	Author      FeedAuthor `json:"author"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	PlayURL     string     `json:"play_url"`
	CoverURL    string     `json:"cover_url"`
	CreateTime  int64      `json:"create_time"`
	LikesCount  int64      `json:"likes_count"`
	IsLiked     bool       `json:"is_liked"`
}
```

这是有意为之，但**代价明确**。有人会问「列表缓存不是通常只存 ID + 少量轻量字段吗」—— 要答出「我知道，并且知道为什么我没那么做」。

**为什么存完整元数据**（而不是按通行建议只存 ID）：

- 关注流是**拉模式**，SQL 直接 JOIN 出整行 —— 内容本来就在结果里，不存白不存
- 结果**因人而异**（含 `IsLiked`）→ 本来就无法跨用户共享 → 「同一视频存 N 份」这个代价躲不掉
- 若只存 ID，每次还要多一次实体装配（MGet）+ 重算 `IsLiked`，缓存省下的就少了

**代价（这是真实的短板）** —— 看 §5 那张失效表，一共只有 3 条，**没有「视频改标题 / 删除 → 失效关注流列表缓存」**：

| 场景 | 实际发生什么 |
|---|---|
| 作者改标题 | 只 `DEL` 了 `video:entity:<id>` → 别人关注流里还是旧标题，**脏到 24h TTL** |
| 作者删视频 | 同样只删实体缓存 → **已删除的视频还会出现在别人关注流里 24 小时** |

**两条修法，各自代价**：

| 方案 | 做法 | 代价 |
|---|---|---|
| **A. 补失效** | 视频变更时 `DelByPattern` 扫掉所有 `feed:listByFollowing:*` | 改一个视频 → 清空**所有人**的缓存，写放大很大 |
| **B. 改结构** | 列表缓存只存 ID，内容走实体缓存 | `IsLiked` 每次还是要算；多一次 MGet |

> **面试讲法**：「这块我是**拿一致性换简单**，24h TTL 兜底。真要改我会选 B —— 因为 A 的失效粒度是『清空全站』，B 的失效粒度是精确的单个实体。」

**同一项目里两种做法都有，判据是「结果是否因人而异」**：

| | 存什么 | 符合「列表只存 ID」的通行建议吗 |
|---|---|---|
| **热榜 / 最新流** | 索引 ZSET 存 **ID + 分数**，内容走实体缓存 | ✅ 符合 —— 快照 key 不含 `accountID`，内容可跨用户共享 |
| **关注流** | 整页缓存含**完整元数据 + IsLiked** | ❌ 不符合，但有理由 |

> 💡 **这题的价值**：它考的是「你会不会根据数据形态判断缓存粒度」，而不是背套路。能说出「同一项目里两种都有、判据是因人而异」，说明是真理解了。

### 8.3 「并联 vs 串联」——判断两套机制该不该共存

这一条能体现你对体系的理解深度 —— **本项目就踩过左边的坑（见 8.1 F5）**：

```
并联（← 本项目踩过，已修复）           合法形态（串联，两层）
                                      
  详情页 ─► SETNX ──► 查库 ─┐           N 个请求 ─► singleflight ─► 每 Pod 1 个 leader
                            ├─► 同一 key                │  （免费收敛 N→P）
  Feed  ─► singleflight ─► 查库 ─┘                      ▼
                                                 这 P 个再去抢 SETNX
  两条路互不知道对方存在                              │  （Redis 代价从 O(N) 降到 O(P)）
  合计 P+1 次回源                                     ▼
                                                 全局只剩 1 次回源
```

**串联合法，并联冗余。** 判据很简单：**两条回源路径感知不到对方，就不叫两层防护。**

但即使串联也只是把分布式锁的成本从 O(N) 压到 O(P) —— 对主键单行查询，按 6.2 的账算下来仍然不划算。所以视频实体的正确收法不是「串联」，而是**直接删掉一层，只留 singleflight**（即 F5 的做法）。

---

### 8.4 生产化清单：还缺什么、为什么现在不做

这一节的价值在于**展示你知道边界在哪**。面试官问「还有没有要补的」，能答出「缺什么 + 为什么不现在做 + 上生产怎么补」，比硬做一堆参数讲不清的东西强得多。

| 项 | 缺什么 | 为什么不现在做 | 上生产怎么补 |
|---|---|---|---|
| **熔断** | DB 延迟/错误率超阈值时快速失败，给下游恢复窗口 | ① 闸门已解决最危险的「并发无上限」② 失败阈值 / 冷却时间 / 半开探测**没有真实流量只能拍脑袋**，被问「为什么是这些值」答不上来 ③ **阈值调错的熔断比没有更糟** —— 正常抖动就熔断，把可用服务搞挂 | 接 `sony/gobreaker`，用压测数据定阈值；或放到网关 / Service Mesh 层做 |
| **Redis 高可用** | 哨兵 / 集群，避免缓存层整体不可用 | ① 这是**部署层**的事，代码侧只能把 `NewClient` 换成 `NewFailoverClient` ② 项目里没有哨兵可连，**改了也验证不了**，写成死代码反而增加复杂度 ③ 架构上**已经假设 Redis 一定会挂**，韧性靠应用层降级建立，不靠 Redis 不挂 | docker-compose 加 sentinel 容器，或直接用云 Redis；客户端换 `NewFailoverClient` |
| **指标与告警** ⭐ | 闸门拒绝数、降级次数、各级命中率的埋点 | 无 Prometheus 接入 | `/metrics` 暴露闸门拒绝计数 —— **这个最该先做**：没有指标，两道闸门的容量只能靠猜 |
| **多实例压测** | 验证 P 个 Pod 下的实际回源量 | 单机项目 | docker-compose 起多副本 + vegeta 压测，校准 `maxConcurrentDBLoad` |

> **一句话收口**（建议背）：
> 「雪崩防护我做了两道闸门 —— 连接池上限和应用侧回源限流，这已经把最危险的『并发无上限』解决了。熔断和 Redis 高可用我知道该做，但这个项目没有真实流量去校准熔断阈值，也没有哨兵环境可验证。**我选择把它们写进生产化清单，而不是硬做一个参数拍脑袋的实现。**」

---

## 9. 面试 Q&A 速答

**Q1：三级缓存每一级的 TTL 分别是多少？为什么？**
L1 = 5 秒（多实例间无失效广播，TTL 即脏数据窗口）；L2 实体 = 1 小时（`video.VideoEntityTTL`，读时按需写入，过期时刻被打散）；L2 列表结果 = 24 小时（查询昂贵，靠主动失效保证一致性）。

**Q2：L2 为什么用 MGet 而不是循环 GET？**
一页 20 个 ID 若循环 GET 就是 20 次网络往返，MGet 压缩成 1 次。代价是整体成功/整体失败，所以降级也是全量的。

**Q3：singleflight 和 SETNX 能互换吗？**
不能。singleflight 只在进程内有效且零 Redis 开销，SETNX 跨实例但每请求要付 Redis 往返。按 6.2 的账目分层：高频廉价的实体读用 singleflight（Feed 列表和详情页**共用同一个 Group**），昂贵低频的关注流用 SETNX。

**Q4：为什么不在写完库后同步更新缓存（写穿透），而是删除？**
删除更简单且避免并发写乱序；下次读时按需回填。代价是下一次读有一次 miss。

**Q5：Redis 挂了怎么办？**
见第 7.3 节话术。核心：不报错、全量降级、singleflight 收敛、恢复后请求驱动自愈。

**Q6：缓存雪崩怎么防？**
① 实体 TTL 1 小时但是**读时按需写入**，过期时刻由各自首次访问时间决定，天然打散；② 热榜快照 key 带分钟级 `as_of`，不同分钟互不影响；③ 真正的入口是 Redis 整体挂掉，靠降级 + singleflight 兜。诚实说：**没有主动加随机抖动**。

**Q7：L1 和 L2 的数据不一致怎么办？**
接受它。把 L1 TTL 压到秒级，把不一致窗口控制在业务可容忍范围内（Feed 对几秒旧数据不敏感）。

**Q8：`GetVideoByIDs` 是不是所有 Feed 接口都用？**
不是，**只有两个调用方**：第 252 行的 `ListLatest`、第 514 行的 `ListByPopularity`。关注流 SQL 直接出整行不需要它。

⚠️ **别再答「热榜没走缓存、属于遗漏」** —— 那是修复前的状态。现在第 514 行已改成 `f.GetVideoByIDs`，与最新流、详情页共用同一份 `video:entity:<id>`（见 §1.1 表格）。这条只适合当作**已修复的复盘素材**讲。

---

## 10. 面试自检：14 题 + 标准答案

> 第 9 节是「速答版」（一句话），这一节是「完整版」（含代码依据和追问方向）。
> 用法：**先用 10.1 裸题自测**，不会的再看 10.2 的答案，最后把 ⭐ 追问也过一遍。

### 10.1 裸题清单（先别看答案）

0. **先定调**：你的 Feed 是推模式还是拉模式？判据是什么？「最新流做了冷热分离」算不算推模式？（见 §0.5）
1. 三级缓存每一级是什么、TTL 多少、为什么是这个值？
2. L1 的实际 TTL 是 3 秒还是 5 秒？为什么代码里两个数不一致？
3. Redis 报错时走哪条分支？是部分降级还是全量降级？为什么？
4. singleflight 为什么必须 safeCopy？两次拷贝分别在闭包内还是外、哪个是必需的？
5. 异步回写 Redis 为什么要用 `context.Background()` 而不是请求的 ctx？
6. singleflight 和 SETNX 各自的适用边界是什么？**把 6.2 的账目算一遍给我听**。
7. 解锁为什么不能直接 `DEL`，而要 Lua 比对 token？
8. 「缓存自愈」在你这个项目里是主动任务还是被动触发？触发点在哪几行代码？
9. `GetVideoByIDs` 有几个调用方？为什么关注流不需要实体缓存？
10. 用户 42 连续上滑产生 20 个几乎同时的请求，被负载均衡分到 10 个 Pod 上（每 Pod 约 2 个）。**最后会查几次库？每个 Pod 上的那 2 个会被合并吗？**
11. `MGet` 调用**成功**了，为什么还会有 key 没命中？那些没命中的接下来完整走什么路？
12. 有人说「列表缓存通常只存 ID + 轻量字段，不该塞完整详情」。你的关注流列表缓存**存了什么**？为什么这么存？代价是什么？
13. Redis 整体宕机，所有请求全量降级到 MySQL。**你怎么避免数据库雪崩？** TTL 随机抖动有用吗？
14. **Bonus**：主动说出任意两个短板 + 改进方案，并讲清 `singleRedisOpTimeout` 这个常量是怎么来的。

---

### 10.2 逐题标准答案

---

#### Q0：先定调 —— 你的 Feed 是推模式还是拉模式？「最新流做了冷热分离」算不算推？

> **拉模式，而且三条流全是拉模式。**
>
> 判据不是「写侧有没有预先准备数据」，而是 **fanout 发生在写还是读、以及 fanout 到谁**：
>
> | | 推（写扩散） | 拉（读扩散） |
> |---|---|---|
> | 发布时 | 扇出到**每个粉丝各自的收件箱** `feed:inbox:<uid>` | 只写**一份** |
> | 写放大 | O(粉丝数)，大 V 发一条 = 百万次写 | **O(1)** |
> | 读放大 | O(1) | O(关注数) |
>
> **硬判据只有一条：有没有 per-user 收件箱。** 本项目全库搜不到 `feed:inbox`，`feed:` 开头的 key 只有 `feed:global_timeline`（全局一份）、`feed:listByFollowing:...`（读时才生成的结果缓存）、`hot:video:*`（全局计数器）—— 都不是收件箱。所以发布一条视频的写放大恒为 O(1)，大 V 零扇出。
>
> **「冷热分离算不算推」——不算。** `feed:global_timeline` 确实是发布时经 Outbox 异步写进去的，但它推的是**全局一份的物化索引**，粉丝关系完全不参与、写放大仍是 O(1)。预写一份全局索引是**加速读的优化**，不是**写扩散**。
>
> 同理，热榜的写侧只做 `ZINCRBY` 加**分数**（`popularity_cache.go:19-26`），排序 / 聚合（`ZUNIONSTORE`）/ 分页全部发生在读请求进来之后 —— 写的是分数，不是内容分发。

⭐ **追问：什么时候你才应该改成推模式？**
→ 关注流读放大扛不住时。改法：给每个用户建 `feed:inbox:<userID>`，发布时异步扇出；**但大 V 不能扇出** —— 微博的推拉结合：普通作者走推，大 V 只写发件箱 `feed:outbox:<authorID>`，读者拉取时单独 merge。答出「大 V 是分界线」= 不是背概念。

⭐ **追问：那你的项目有几个「获取视频」的入口？**
→ 8 个（`/feed` 五个含前端未接的 `listByTag`、`/video/listByAuthorID`、`/video/getDetail`、`/like/listMyLikedVideos`），详见 §0.5.5。可主动补短板：**没有搜索接口**。

完整版见 **§0.5**。

---

#### Q1：三级缓存每一级是什么、TTL 多少、为什么是这个值？

> 三级都实现在同一个函数里：`FeedService.GetVideoByIDs`（第 44 行）。
>
> | 层 | 实现 | TTL | 逻辑 |
> |---|---|---|---|
> | L1 | 进程内存 `patrickmn/go-cache` | **5 秒** | 多个 API 实例之间**没有任何失效广播**，所以 TTL 就是「脏数据窗口」的上界。设长了，实例 A 感知不到实例 B 触发的写；设短了失去本地缓存的意义。Feed 对几秒旧数据不敏感，5 秒是折中 |
> | L2 | Redis `video:entity:<id>` | **1 小时**（`video.VideoEntityTTL`） | **这是兜底上界而不是常态** —— 写路径会主动 `DEL`。而且它是**读时按需写入**，过期时刻由各自首次访问时间决定，天然打散 |
> | L3 | MySQL | —— | 兜底数据源 |
>
> **最容易混的一点**：L1 的 TTL = 脏数据窗口（TTL 驱动收敛）；L2 的 TTL ≠ 脏数据窗口（**事件驱动**失效，常态是毫秒级）。

**代码依据**：第 55 行（L1 key）、第 77 行（L2 key）、第 123 行（L3 查库）。

⭐ **追问：既然 L2 靠 DEL 失效，为什么还要设 1 小时 TTL？**
→ 兜底。`DEL` 的错误是被 `_ =` 丢弃的，万一失败，旧数据最多活到 TTL 到期，不会永久脏。

---

#### Q2：L1 的实际 TTL 是 3 秒还是 5 秒？为什么代码里两个数不一致？

> **实际是 5 秒。**
>
> ```41:41:backend/internal/feed/service.go
> 	return &FeedService{repo: repo, likeRepo: likeRepo, rediscache: rediscache, localcache: cache.New(3*time.Second, 5*time.Second), cacheTTL: 24 * time.Hour}
> ```
>
> `cache.New(defaultTTL, cleanupInterval)` 的两个参数是「**默认 TTL**」和「**后台清理 goroutine 的扫描间隔**」—— **5 秒是扫描间隔，不是 TTL**。
>
> 真正决定 TTL 的是两处写入显式传的参数（第 94、148 行都传 `5*time.Second`），go-cache 里显式 TTL 优先于默认 TTL。所以：
> - 实际生效 TTL = **5 秒**
> - 3 秒这个默认值**根本没用上**（因为没有调用过 `SetDefault`）

⭐ **这一题的价值在于验证你是不是真读过代码。** 主动说出「很多人以为是 3 秒，实际是 5 秒」，比答对本身更值钱。

---

#### Q3：Redis 报错时走哪条分支？部分降级还是全量降级？为什么？

> **全量降级。**
>
> ```102:106:backend/internal/feed/service.go
> 		} else {
> 			// 如果 Redis 挂了或者超时了，全部降级到 L3
> 			missedL2 = missedL1
> 			log.Printf("L2 Redis MGet 失败，全部降级到 MySQL: %v", err)
> 		}
> ```
>
> 判断逻辑是 `if err == nil` 才解析结果，非 nil（连接错误、超时）直接把整批 `missedL1` 推到 `missedL2`。
>
> **为什么不能部分降级**：因为 L2 用的是 `MGet` —— 一次调用拿几十个 key，**要么整体成功要么整体失败**。调用本身报错时连返回切片都没有，不存在「一半拿到一半没拿到」。

⚠️ **别混淆两种「miss」**（详见 3.3 / 3.4）：

| 情况 | 处理 | 粒度 | 性质 |
|---|---|---|---|
| `MGet` 调用报错/超时 | 整批沉到 MySQL（第 104 行） | **全量** | **降级** —— Redis 不可用，被迫绕过它 |
| `MGet` 成功但个别 key 是 `nil` | 第 87 行 `res != nil` 逐个判断，进 `missedL2` | **逐条** | **普通 miss** —— Redis 正常，只是这几个 key 是冷的 |

**这两行不矛盾** —— 能不能逐条处理，取决于「你手上有没有逐条的信息」：

```
报错 → 连数组都没有 → 0 信息     → 只能保守地全都当 miss
成功 → 拿到完整逐 key 结果 → 信息完整 → 精确知道哪几个 miss
```

⚠️ **用词要准**：第二行严格讲**不是「降级」**，是正常的缓存未命中。两者最终都走到 L3 查库并回填，但**原因完全不同** —— 一个是「Redis 废了被迫全查」，一个是「Redis 好好的，只是这几个 key 冷」。

💡 **`MGet` 成功了为什么还会有 nil？** → 因为 Redis 里 `nil` 就是「key 不存在」的表示，不是错误。`err == nil` 只说明命令执行成功，不代表所有 key 都命中。详见 3.3。

⭐ **追问 1：Redis 超时怎么控制？**
→ 第 80 行 `context.WithTimeout(ctx, singleRedisOpTimeout)`，50ms 硬超时，保证 Redis 抖动不拖垮接口响应时间。

⭐ **追问 2：降级后 DB 扛得住吗？**
→ 三层缓冲：L1 的 5 秒本地缓存 + singleflight 按 ID 去重（一页 20 个 ID 最多 20 次查询，而不是 20×并发数）+ 上层 IP/账号维度限流。但要诚实补充：**没有熔断、没有限流兜底到 DB 这一层**，这是「可用性优先于 DB 保护」的取舍。

---

#### Q4：singleflight 为什么必须 safeCopy？两次拷贝分别在闭包内还是外、哪个是必需的？

> **前提**：singleflight 的 `Do` 返回的是**同一个指针**，leader 和所有 follower 共享那一块内存。
>
> **第一次（第 129 行，闭包内）—— 防御性的，当前非必需**
> ```129:129:backend/internal/feed/service.go
> 				safeCopy := *videoList[0]
> ```
> 隔离「回源对象」与「写进 Redis 的序列化快照」。之所以说非必需，是因为 `json.Marshal` 在第 131 行**同步执行**，异步 goroutine 拿到的是已经序列化好的 `[]byte`（第 133 行按参数传进去，不捕获外部变量）。它防的是「将来有人把 Marshal 挪进 goroutine」这种常见腐化。
>
> **第二次（第 144 行，闭包外）—— 必需的**
> ```144:144:backend/internal/feed/service.go
> 				safeCopy := *(v.(*video.Video))
> ```
> 第 140 行 `return videoList[0]` 返回的是指针，100 个 follower 拿到同一个对象 —— 而这 100 个请求是**不同用户**的。任何一处往这个对象上写个性化字段（比如 `IsLiked`），就会串到另外 99 个用户的响应里。
>
> 顺带第 148 行写 L1 时传的是 `safeCopy` **值**而不是指针：go-cache 存 `interface{}`，塞指针进去以后任何修改都会反向污染缓存。

⭐ **追问：那第一次拷贝能直接删吗？**
→ 诚实答：「当前下游 `buildFeedVideos` 只读不写，严格讲不拷贝也跑得通。但这是把正确性建立在『后续没人修改这个对象』的约定上，拷贝成本几十字节，不值得省。」

---

#### Q5：异步回写 Redis 为什么要用 `context.Background()` 而不是请求的 ctx？

> ```131:139:backend/internal/feed/service.go
> 				if b, err := json.Marshal(safeCopy); err == nil {
> 					//异步回写redis
> 					go func(k string, b []byte) {
> 						setCtx, setCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
> 						defer setCancel()
>
> 						f.rediscache.SetBytes(setCtx, k, b, video.VideoEntityTTL)
> 					}(cachekey, b)
> 				}
> ```
>
> 两个原因：
>
> 1. **生命周期不等长**：请求的 `ctx` 在 handler 返回时就被 gin 取消了。异步 goroutine 通常比请求跑得慢，如果继承 `ctx`，取消信号一到 `SetBytes` 立刻失败 —— **回源成功但缓存永远写不进去**，下一次请求还得重走一遍三级链路。用 `Background()` 让它脱离请求生命周期。
> 2. **目的不同**：这里要的是「尽快写完，失败就算了」，而不是「跟随请求取消」。
>
> 注意它不是无限等待 —— 仍然套了 `singleRedisOpTimeout`（50ms），最多占用 Redis 50ms；`defer setCancel()` 保证 goroutine 退出时释放资源。

⭐ **追问：那用 `context.WithoutCancel` 不更好？**
→ Go 1.21 才有这个 API，`Background()` + 显式 timeout 是等价的低版本写法。

---

#### Q6：singleflight 和 SETNX 各自的适用边界是什么？把 6.2 的账目算一遍给我听。

> **这是本专题最值钱的一题，建议脱稿。**
>
> 设 `N` = 同一 key 的并发请求数、`P` = Pod 数、`C` = 单次回源成本、`R` = 一次 Redis 往返（≈0.3ms）。
>
> **两种方案的账单**：
>
> | | singleflight | SETNX |
> |---|---|---|
> | Redis 开销 | **0**（纯内存 map） | 每个请求 1~N 次往返 |
> | DB 查询次数 | **P 次**（每 Pod 放行 1 个） | 1 次（理想情况，轮询超时的会漏） |
> | follower 等待时长 | = leader 查库耗时 | 轮询粒度起跳，上限 100ms |
>
> **关键等式**：
> ```
> SETNX 比 singleflight 多买到的 = 省掉那 (P-1) 次回源 = (P-1) × C
> SETNX 比 singleflight 多付出的 = N × k 次 Redis 往返 + 每个 follower 最多 100ms
> ```
> 因为 **N >> P**（1000 个请求 vs 10 个 Pod），右边天然很大 —— **除非 C 也很大，否则这笔买卖永远亏。**
>
> **代入数字**（P=10，N=1000，R=0.3ms）：
>
> | 场景 | 省下的 | 付出的 | 结论 |
> |---|---|---|---|
> | 视频详情 / 实体，C≈0.1ms | 9×0.1 = **0.9ms** DB | 1000×2 次往返 ≈ **600ms** Redis | **亏到离谱** |
> | 关注流 JOIN，C≈50ms | 9×50 = **450ms** DB | 600ms Redis（但 Redis 比 MySQL 廉价） | **划算** |
>
> **还有两个和成本无关的理由**：
> 1. **follower 拿到结果的方式不同** —— singleflight 的 follower 直接拿 leader 的返回值，**不经过 Redis**；SETNX 的 follower 必须轮询 Redis，一旦 leader 回填失败，轮询全部落空，最后还得自己查库 —— **Redis 的钱付了，DB 一次没省**。
> 2. **可用性依赖** —— Redis 宕机时 SETNX 完全失效，singleflight 是**唯一还活着的防线**，而热点路径恰恰必须保住。
>
> **本项目结论**：实体读用 singleflight（Feed 列表 `GetVideoByIDs` 与详情页 `GetDetail` **共用同一个 `EntityLoadGroup`**）；关注流整页用 SETNX。

⭐ **追问：那就是放着「更弱」的方案不用 stronger 的？**
→ 「『更弱』这个直觉本身就错了。singleflight 在『Redis 开销』这个维度上是**更强**的，只在『合并到几个』这个维度上弱。选哪个取决于**哪个维度贵**。」

---

#### Q7：解锁为什么不能直接 `DEL`，而要 Lua 比对 token？

> **经典误删场景**：
> 1. A 拿到锁，业务执行超过锁的 TTL（本项目 500ms）；
> 2. 锁自动过期，B 拿到锁；
> 3. A 执行完直接 `DEL` → **把 B 的锁删了**，C 又能进来，互斥彻底失效。
>
> 所以解锁必须满足两个条件：
> - **只删自己的锁**：用随机 token 标识持锁者；
> - **比对和删除必须原子**：放在同一个 Lua 脚本里，分开做 `GET` 和 `DEL` 中间仍然会被插入。
>
> ```82:88:backend/internal/middleware/redis/redis.go
> var unlockScript = redis.NewScript(`
> if redis.call("GET", KEYS[1]) == ARGV[1] then
>   return redis.call("DEL", KEYS[1])
> else
>   return 0
> end
> `)
> ```
>
> 加锁同理不能分开做：
> ```78:78:backend/internal/middleware/redis/redis.go
> 	ok, err = c.rdb.SetNX(ctx, key, token, ttl).Result()
> ```
> `SET key value NX PX ttl` 一条命令原子完成「不存在才写 + 带过期」，避免先查再写的竞态。

⭐ **追问：业务执行超过 TTL 这个根本问题怎么解？**
→ 锁的自动续期（Redisson 的 watchdog）。本项目没做，靠 500ms TTL + 「轮询超时就自己查库兜底」来容忍这个缺陷 —— 一致性换简单。

---

#### Q8：「缓存自愈」在你这个项目里是主动任务还是被动触发？触发点在哪几行代码？

> **被动的、请求驱动的。没有任何定时任务、没有预热脚本。**
>
> | 阶段 | 发生了什么 | 代码 |
> |---|---|---|
> | ① Redis 故障 | `MGet` 报错/超时 → `missedL2 = missedL1` → 全部走 MySQL，请求**正常返回不报错** | 第 102-106 行 |
> | ② 故障期间 | singleflight 按 ID 合并回源，同一视频同一时刻最多 1 次 DB；L1 仍提供 5 秒窗口；异步回写静默失败 | 第 122、131-139 行 |
> | ③ Redis 恢复 | 后续请求照常走三级链路，miss 的 ID 回源后**异步回写**，缓存逐步填满 | 第 131-139 行 |
>
> **触发点就是第 131-139 行那个异步 `SetBytes`** —— 每个走到 L3 的请求都会顺手把缓存补回去，所以是「谁先访问谁负责重建」。
>
> 优点：重建粒度天然跟随真实热点，不会去加载冷数据。
> 代价：恢复瞬间 DB 有一波压力上升。

⭐ **追问：为什么不主动预热？**
→ 请求驱动的粒度天然跟随热点；预热可能加载一堆没人看的数据。

⭐ **追问：自愈期间会不会把旧数据写回缓存？**
→ 不会。故障期间读的是 MySQL 最新值，回写的也是最新值，是一致的。

---

#### Q9：`GetVideoByIDs` 有几个调用方？为什么关注流不需要实体缓存？

> **全项目有两个调用方** —— 第 252 行的 `ListLatest`、第 514 行的 `ListByPopularity`。
>
> | 接口 | 用吗 | 为什么 |
> |---|---|---|
> | `ListLatest` 最新流 | ✅ | ZSET `feed:global_timeline` 里**只有 ID**（`videoID → 时间戳`），没有标题没有封面，必须再 hydrate 一次 |
> | `ListByPopularity` 热榜 | ✅ | 快照 ZSET 只给 **ID 顺序**，内容要另外按 ID 装配（原本是遗漏，已补齐） |
> | `ListByFollowing` 关注流 | ❌ | MySQL **直接 SELECT 出 videos 整行**，内容已经有了，不存在「先拿 ID 再装配」这一步，实体缓存无处插入 |
>
> **本质规律**：只有「先拿到 ID 列表、再按 ID 装配内容」的路径才需要实体缓存。
>
> ~~对应到推拉模式：推模式（ZSET 只存 ID）→ 必须 hydrate；拉模式（SQL 出整行）→ 不需要。~~
> ⚠️ **这层对应关系是不严谨的，别照着答**（详见 §0.5.4）：ZSET 存 ID 只决定「要不要 hydrate」，**不决定推还是拉**。本项目三条流全是拉模式，但最新流与热榜照样要 hydrate。推/拉的判据只有一条：**有没有 per-user 收件箱、写放大是 O(1) 还是 O(粉丝数)**。
>
> 所以关注流改成缓存**整页结果**（`feed:listByFollowing:limit=...:accountID=...:before=...`，24h + SETNX），而不是缓存单个实体 —— 因为它的查询昂贵且因人而异。

⭐ **追问：热榜为什么用实体缓存，而不是像关注流那样缓存整页？**
→ 三个理由：
> ① **结构天然匹配** —— 热榜的「排序」和「内容」本来就是分离的：ZSET 快照只给出 ID 顺序，内容另外装配，这正是实体缓存的适用形态。
> ② **全局共享，不因人而异** —— 热榜快照 key 不带 `accountID`，所有人看同一份，不需要「每人一份」。
> ③ **整页缓存反而不合适** —— 热榜翻页参数有五个（`offset` / `asOf` / `latestPopularity` / `latestBefore` / `latestIDBefore`），组合爆炸命中率极低；而且 ID 列表已经被 ZSET 快照缓存过了，再缓存整页是重复缓存；内容装配就是主键批量查询，本来就不贵。
>
> 对比关注流：它**查询昂贵**（JOIN 关注表 + 排序）且**因人而异**，所以适合整页缓存。两个接口选型的判据不一样。

---

#### Q10：20 个请求散在 10 个 Pod 上，最后会查几次库？每个 Pod 上的 2 个会被合并吗？

> **最后只查 1 次库。每个 Pod 上的那 2 个不会被合并。**
>
> **先说「不会被合并」** —— 关注流这条路径（`ListByFollowing`，第 346-462 行）**根本没有 singleflight**，只有 SETNX。所以同一个 Pod 上的 A、B 两个请求各自独立走流程：
>
> ```
> A: GET 缓存 → miss → SETNX → 0（没抢到）→ 轮询
> B: GET 缓存 → miss → SETNX → 0（没抢到）→ 轮询       ← A、B 互相不知道对方存在
> （某个 Pod 上的请求 C 抢到了锁 → 查库 → 回填）
> A / B: 下一次轮询读到缓存 → 返回
> ```
>
> **再说「只查 1 次」** —— 因为那把锁在 Redis 里是**全局的**，20 个请求里只有一个能 `SETNX` 成功。B 拿到结果**不是因为被合并**，而是**被锁挡住后轮询读到了缓存** —— 机制完全不同，别混着说。
>
> ⚠️ **赢的可能是任意一个 Pod 上的任意一个请求**，跟「哪个 Pod」无关，只跟谁先执行到 SETNX 有关。
>
> **为什么这条路径不用 singleflight**：它的 key 带 `accountID + limit + 游标`，**每个用户每页都不一样**，同一个 key 的并发天然极低（一个用户通常一次就发一个请求）。所以并发形态是「**跨 Pod 稀疏**」—— 同 key 请求散落在 10 个 Pod 上、每 Pod 就一两个。singleflight 是进程内的，管不到别的 Pod；SETNX 是全局的，正好覆盖。

⭐ **追问：那加一层 singleflight（外层合并、内层抢锁）会不会更好？**
→ 不会更好，见 6.5 末尾的账：DB 查询次数**本来就是 1**，融合只能把 SETNX 调用从 20 次降到 10 次（省约 3ms Redis），却多一层复杂度。而正常情况下同 Pod 同 key 并发就是 1，收益接近 0。

---

#### Q11：`MGet` 调用成功了，为什么还会有 key 没命中？那些没命中的接下来怎么走？

> **因为在 Redis 里 `nil` 就是「key 不存在」的标准表示，它不是错误。**
>
> `GET` 一个不存在的 key 返回 nil，`MGet` 是「批量 GET」，把每个 key 的结果逐个平铺成数组，缺失的位置填 `nil`。**数组长度永远等于传入的 key 数量。**
>
> > **`err == nil` 只表示「这条命令执行成功了」，不代表「所有 key 都命中了」。**
>
> 落到 `missedL2` 的其实有**三种**：`res == nil`（不存在）、类型断言失败、JSON 解析失败（缓存损坏）—— 后两种也被当 miss 重新回源，所以脏数据不会打断请求。
>
> **没命中的那些接下来完整走什么路**：
>
> ```
> 收进 missedL2（第 100 行）
>   → 每个 ID 起一个 goroutine（第 116-151 行），并发查 MySQL
>        └─ 每个都过 EntityLoadGroup 按 ID 去重（第 122 行）
>   → 查到后回填两层：L1 本地（第 148 行，同步，5s）
>                    L2 Redis（第 131-139 行，异步 SetBytes，1h）
>   → buildOrderedResult（第 598 行）按入参原始顺序拼回
>        └─ 不会因为这 3 个走了查库就乱序
> ```
>
> **如果 MySQL 里也查不到**：第 125-127 行返回 `(nil, nil)` → 结果里没有这个视频，而且**下次还会再查一次库** —— 这就是 8.2 的穿透问题。

⭐ **追问：为什么用 `MGet` 而不是循环发 N 次 `GET`？**
→ `MGet` 是 1 次往返（≈0.3ms），N 次 `GET` 是 20 次往返（≈6ms，**20 倍**）。代价是「全有或全无」—— 报错时整批降级。但因为 Redis 故障通常是全局的（不是某几个 key 单独失败），用 1 次往返换掉「部分失败隔离」是划算的。详见 3.4。

---

#### Q12：有人说「列表缓存只该存 ID + 轻量字段」，你的关注流缓存存了什么？为什么？

> **存的是完整元数据，而且比「完整详情」还多一个 `IsLiked`。**
>
> ```10:20:backend/internal/feed/entity.go
> type FeedVideoItem struct {
> 	ID          uint       `json:"id"`
> 	Author      FeedAuthor `json:"author"`
> 	Title       string     `json:"title"`
> 	...
> 	IsLiked     bool       `json:"is_liked"`
> }
> ```
>
> **为什么这么存**（明知通行建议是只存 ID）：
> - 关注流是**拉模式**，SQL 直接 JOIN 出整行 —— 内容本来就在结果里
> - 结果**因人而异**（含 `IsLiked`）→ 本来就无法跨用户共享 → 「同一视频存 N 份」这个代价躲不掉
> - 若只存 ID，每次还要多一次实体装配（MGet）+ 重算 `IsLiked`
>
> **代价（诚实说）**：§5 的失效表只有 3 条，**没有「视频改标题 / 删除 → 失效关注流列表缓存」**。所以作者改标题后，别人的关注流里还是旧标题，**脏到 24h TTL**；删视频同理，已删除的视频还会在别人关注流里待 24 小时。
>
> **两套管法**：A 补失效（`DelByPattern` 扫 `feed:listByFollowing:*`，代价是清空全站）；B 改结构（只存 ID，内容走实体缓存，失效粒度精确）。**我选 B。**

⭐ **追问：那热榜/最新流为什么就没这个问题？**
→ 因为它们的索引 ZSET **只存 ID + 分数，不存内容**，内容走共享的 `video:entity:<id>` —— 快照 key 不含 `accountID`，内容可跨用户共享，视频变更时 `DEL` 一次就全生效。
> **同一项目里两种做法都有，判据是「结果是否因人而异」。**

---

#### Q13：Redis 整体宕机，所有请求全量降级到 MySQL，怎么避免数据库雪崩？TTL 抖动有用吗？

> **先说 TTL 抖动：对这种雪崩完全无效。** 它防的是成因 (a)，而这里是成因 (b) —— Redis 已经不响应了，key 过没过期根本不重要。
>
> **雪崩有两个成因，必须分开看**：
>
> | 成因 | 解法 |
> |---|---|
> | (a) 大量 key **同时过期** | TTL 随机抖动 + 多级缓存 |
> | (b) 缓存层**整体不可用** | 熔断 + 限流 + 兜底（**抖动无效**） |
>
> **本项目 Redis 挂掉时发生什么**：第 151-155 行 `MGet` 报错 → `missedL2 = missedL1` → 全量回源。此时 **singleflight 只能把 N 收敛到 P（Pod 数），不是 1**；而 SETNX 锁因为依赖 Redis，**此时已经失效**。
>
> **加的防护 —— 补两道闸门，缺一不可**：
> - **驱动层**：`sqlDB.SetMaxOpenConns(64)` —— 原来 GORM 默认 `MaxOpenConns = 0`（无上限），会一路建连接直到打满 MySQL 的 `max_connections`，那会让**所有**查询一起失败
> - **应用层**：`dbLoadGate`，容量 `config.DefaultMaxConcurrentDBLoad = MaxOpenConns / 2 = 32`（另一半留给写路径、事务、迁移和其他包）
> - 应用层闸门**非阻塞**：拿不到配额直接放弃本次回源，**不排队** —— 排队只是把堆积从数据库搬到应用内存，压力最终还是会落到库上
> - 降级表现：实体路径该条不返回（其余照常）；关注流是整页昂贵 JOIN，没法部分返回，回 `errDBOverloaded` → **503**
>
> **为什么 503 不是 500**：`errDBOverloaded` 包装 `apierror.ErrServiceOverloaded`，`ClassifyHTTPStatus` 映射成 503。这是**主动限流降级**不是服务故障 —— 客户端收到 503 才知道该退避重试。
>
> **还差什么（诚实说）**：熔断（DB 延迟超阈值就快速失败）和 Redis 高可用（哨兵/集群）**有意识地没做** —— 没有真实流量校准不了熔断阈值，没有哨兵环境验证不了高可用；**宁可写进生产化清单，也不硬做一个参数拍脑袋的实现**。详见 §8.4。

⭐ **追问：singleflight 为什么不能解决多个 Go 实例之间的击穿？**
→ 因为它的状态是**进程内的 map**（`Group.m map[string]*call`）。不同 Pod 是**独立的操作系统进程**，内存地址空间完全隔离 —— Pod A 的 map 里**不可能**有「Pod B 正在加载这个 key」的记录。所以只能 **N → P，不是 N → 1**。
> 要跨实例合并，必须把「谁在加载」这条信息搬到**所有实例都看得见的地方** —— 那就是 Redis（SETNX 分布式锁）。代价是每请求 2+ 次 Redis 往返。
>
> **那实体缓存为什么故意不用 SETNX**？6.2 的账算过：主键查询 C≈0.1ms，用 SETNX 保护它亏到离谱（省 0.9ms DB，付 600ms Redis），等待者还得多等 20~100ms、**延迟放大 200~1000 倍**。
> 更关键的是第三个理由：**Redis 宕机时，singleflight 是唯一还活着的防线** —— 如果实体缓存用了 SETNX，Redis 一挂，锁也跟着废了。

---

#### Bonus：短板 + `singleRedisOpTimeout` 的来历

**短板（任选两个，详见第 8.2 节）：**

1. **关注流整页缓存含完整元数据，视频变更无失效**（第 455-458 行）：作者改标题/删视频后，别人的关注流里仍是旧数据，**脏到 24h TTL**。改法二选一：A 补 `DelByPattern`（代价是清空全站缓存）；B 改成只存 ID、内容走实体缓存（失效粒度精确）—— **我选 B**。
2. **`ListByPopularity` 的 `opCtx` 被 4 步共用**（第 479 行）：`Exists` + `ZUnionStore`（合并 60 个 key，最贵）+ `Expire` + `ZRevRange` 共用 80ms。改法：每步独立 ctx，`ZUnionStore` 给更长预算。
3. **singleflight 用 `Do` 不用 `DoChan`**（第 122/175/223/270 行）：`Do` 不接受 ctx，leader 卡死则所有 follower 永久阻塞。改法：换 `DoChan` + `select{ctx.Done(); time.After()}` + 超时兜底查库。

**`singleRedisOpTimeout` 怎么来的（推荐作为整场面试的收尾）：**

> 「原来关注流那个分布式锁分支，我把一个 50ms 的 context 同时用在『加锁 → 双重检查 → 回填』上。带 timeout 的 context 的额度会被串联在上面的操作**连续消耗**，本地低负载看不出来，DB 一慢回填就超时失败、错误还被 `_ =` 吞掉，结果缓存永远写不进去，锁退化成串行查库。
> 修法是每一步 Redis 操作各自申请独立预算，抽成了这个常量。从这件事我记住一个原则：**带 timeout 的 context 不能跨多个 IO 步骤复用。**」

01

为什么本地缓存只设置秒级 TTL？

考察多实例一致性与延迟的权衡。


02

为什么实体缓存使用 singleflight，而列表缓存使用 SETNX？

考察进程内去重和跨实例互斥的边界。
为什么实体缓存用 singleflight？
实体缓存（比如单条视频/文章详情 feed:item:{id}）的特点是：高并发读、缓存失效瞬间容易击穿。
比如某条热门内容缓存刚好过期，1000 个请求同时 miss，如果不做控制，就会同时打到数据库。singleflight 做的事情就是：同一个 key 只让一个请求去回源查库，其余请求原地等待、共享结果。
它本质上是进程内的并发请求合并，解决的是"缓存 miss 时如何压住回源流量"这个问题。
为什么列表缓存用 Redis SETNX？
列表缓存（比如 feed:public:ids:{size}:{hourSlot}:{page}）的特点不同：
它是分页数据，缓存失效时往往涉及整页重建，成本比单条实体更高
Feed 流通常多实例部署，singleflight 只能拦住单 JVM 内的并发，跨实例无效
列表缓存需要跨实例协调，保证同一时刻只有一个实例在做回源+写缓存
所以用 Redis SETNX（或 SET NX EX）做分布式锁，抢到锁的实例负责回源写缓存，其他实例等待或降级读取旧数据。
一句话总结
实体缓存	列表缓存
实体缓存	列表缓存
工具	singleflight	Redis SETNX
作用域	单进程内	跨实例分布式
解决什么	缓存击穿时压住回源	多实例并发重建时互斥
为什么选它	轻量、零网络开销、结果共享	全局可见、原子性保证
简单说就是：singleflight 管"同一台机器上的重复劳动"，SETNX 管"多台机器之间的互斥竞争"。两者配合，才能在高并发下既防击穿又防多实例重复回源。
03

Redis 故障时全量回源，MySQL 怎么不被打垮？

考察方案是否考虑了故障流量。


04

先删缓存还是先更新数据库？如何解决并发回填旧值？

考察缓存一致性的真实理解。


05

没有拿到 SETNX 锁的请求怎么处理？

考察锁竞争下的请求行为。


06

MGet 与 MySQL 批量查询分别怎么实现？

考察实际代码路径和批量优化。


07

Redis 恢复后的异步回填，如何防止旧值覆盖新值？

考察恢复流程的并发安全。


08

你如何证明热点请求不再穿透 MySQL？

考察监控、压测与数据真实性。  