# 三级缓存面试问题汇总（17 题）

> 对应题库：`chatgpt的面试问题设计.md` 第一章「三级缓存与缓存自愈」
> 代码基线：**2026-10-27** 的 `backend/internal/feed/service.go`、`backend/internal/video/*`
> 配套文档：`面试/Feed三级缓存与缓存自愈-面试专题.md`（完整专题）、`start-learn/缓存体系.md`（源码笔记）、`learn/feed模块学习文档.md`（模块总览）
> 用法：先看 §0 的 7 条事实 → 按题号背 → 用 §4 一页速答复盘 → §6 检查旧文档里哪些话已经过时
>
> **2026-10-27 修订**：文中标注「已修」的条目已落到代码：统一回源闸门（`dbguard`）、实体回写版本 CAS、关注流列表只存 ID、hydrate 失败不再静默短页、Redis `ContextTimeoutEnabled`、改名同步 `videos.username`、有界异步回写。下面的事实与代码一致，旧的「本项目没做」表述已逐条更新。

---

## 0. 先钉死 7 条事实（否则后面每道题都会答偏）

**事实 1：三级缓存只覆盖「按 ID 装配视频实体」这一条路径。**
`GetVideoByIDs` 有三个调用方：`ListLatest`、`ListByPopularity`、`ListByFollowing` 的 hydrate。关注流的**列表缓存只存 `VideoIDs + 游标 + HasMore`**，内容每次都用同一套三级实体缓存装配，不再冻结整页渲染结果。

**事实 2：实体缓存 `video:entity:<id>` 由三条读路径共享。**
Feed 最新流、热榜、详情页 `GetDetail` 共用同一个 key、同一个 TTL、同一个 singleflight Group。这是 10 月那轮重构的结果（旧文档里的 `video:detail` 和详情页 SETNX 已经不存在）。

**事实 3：L1 实际 TTL 是 5 秒，不是 3 秒。**
`cache.New(3*time.Second, 5*time.Second)` 的两个参数是「默认 TTL」和「janitor 扫描间隔」；两处写入都显式传了 5 秒，显式值优先。

**事实 4：穿透、雪崩和「旧值回填」都已有实现。**
- 穿透：`video:entity:<id>` 写 `__null__` 哨兵，TTL 60s，Feed 和 `GetDetail` 都识别。
- 雪崩：MySQL 连接池 64 + 统一回源闸门 `dbguard`（容量 = `MaxOpenConns/2` = 32/进程，非阻塞）。
- 旧值回填：每次失效 `INCR version + DEL entity`（Lua 原子），回写用版本 CAS；版本读不到就不回写。
- 异步回写：有界（256 个在途槽位，满了丢弃），避免 Redis 故障时 goroutine 无上限堆积。

**事实 5：50ms 现在是真的 50ms 上限。**
`redis.NewFromEnv` 已开 `ContextTimeoutEnabled: true`，所有 `WithTimeout(50ms)` 会真正约束 socket I/O；同时把 `ReadTimeout/WriteTimeout` 压到 1s 作为无 deadline 调用的兜底。旧代码默认 3s 读超时的问题已修。

**事实 6：本项目没有主动「预热」，自愈是请求驱动的。**
Redis 恢复后，谁先访问谁回源并异步回填；没有定时任务，也没有预热脚本。

**事实 7：关注流不再缓存整页渲染结果，但仍不是强一致。**
列表缓存只有 ID 和游标：改名、点赞数、`IsLiked`、删视频都会在 hydrate 时重新计算，不再脏 24 小时。剩下的窗口是：
- 首页 TTL 30s（新视频/取关变化），深层页 24h（游标单向翻页，新内容不影响已翻过的页）；
- L1 仍是每实例一份，写路径只删 Redis，跨实例最多脏 5s（Q15）。

---

## 0.1 一张链路图

```text
GetVideoByIDs(videoIDs)                      ← 最新流 / 热榜 / 关注流 hydrate
  │
  ├─ L1  go-cache（进程内存，实际 5s）
  │     命中 → 直接进 videoMap
  │     未命中 → missedL1
  │
  ├─ L2  Redis MGET（1 次批量，50ms ctx）
  │     命中 → 反序列化 → 回写 L1(5s) → videoMap
  │     哨兵 __null__ → 跳过，不回源
  │     未命中/类型错/JSON 坏 → missedL2
  │     MGet 报错/超时 → missedL2 = missedL1（全量降级）
  │
  ├─ 版本 MGET：取 missedL2 各自的 video:entity:ver:<id>
  │     读失败 → 本次不回写（比较基准不可信）
  │
  └─ L3  MySQL：每个 miss 的 ID 一个 goroutine
        └─ singleflight.Do("sf:entity:<id>")
             ├─ 抢回源闸门 dbguard（32/进程，非阻塞）
             ├─ leader：查库
             │    ├─ 查到 → 有界异步回写：版本 CAS 成功才 SET(1h)
             │    └─ 查不到 → 有界异步写 __null__ 哨兵(60s)
             └─ follower：阻塞等待，共享 leader 的返回值
        → 每个请求各自写 L1(5s) → videoMap
  │
  ▼
buildOrderedResult(videoIDs, videoMap)       ← 按原顺序重排
        │
        └─ 有 ID 回源失败 → 返回聚合错误（handler 映射 500/503），
           不再返回「短页 + nil error」
```

关注流的列表层是另一份缓存，但只到 ID 为止：

```text
ListByFollowing(limit, cursor, accountID)
  ├─ Redis GET feed:listByFollowing:v2:...  → CachedFollowingPage{VideoIDs, NextTime, NextIDBefore, HasMore}
  │     命中 → GetVideoByIDs(IDs) → 重新装配 LikesCount / IsLiked / 已删 / 改名
  └─ miss → SETNX 锁 → dbguard → JOIN 查询 → 只缓存 ID 页（首页 30s / 深层 24h）
```

对应代码：`backend/internal/feed/service.go:123-263`、`backend/internal/video/video_cache.go`、`backend/internal/videocache/entity.go`。

---

## 0.2 参数总表（面试前必须能脱口而出）

| 参数 | 值 | 位置 | 作用 / 为什么 |
|---|---|---|---|
| L1 默认 TTL | 3s | `feed/service.go:118` | 未生效，模块里没有调用 `SetDefault` |
| L1 janitor 扫描间隔 | 5s | `feed/service.go:118` | 后台清理过期条目的频率，不是 TTL |
| L1 实际 TTL | **5s** | `feed/service.go:178,289` | 显式传入，优先于默认值 |
| 实体缓存 TTL | **1h** | `videocache/entity.go:19` | 兜底上界；一致性主要靠写路径 `DEL` |
| 空值哨兵 TTL | **60s** | `videocache/entity.go:28` | 防穿透；数据补上后最多延迟 60s 可读 |
| 版本号 TTL | **25h** | `videocache/entity.go:33` | 必须长于实体 TTL，否则迟到的旧值又能通过 CAS |
| 关注流首页 TTL | **30s** | `feed/service.go:43` | 首页会被新视频影响，取关也会整体失效 |
| 关注流深层页 TTL | **24h** | `feed/service.go:653` | 游标单向翻页，新内容不会改变已翻过的页 |
| 单次 Redis 操作 ctx | **50ms** | `feed/service.go:28`、`video/video_cache.go:19` | 已开启 `ContextTimeoutEnabled`，真实生效 |
| Redis 读/写超时 | **1s** | `middleware/redis/redis.go` | 无 deadline 调用的兜底上限 |
| 关注流锁 TTL | **500ms** | `feed/service.go:710` | 防止持锁进程挂掉后死锁 |
| 等锁轮询 | **5 × 20ms** | `feed/service.go:730-739` | 等不到就兜底查库，不让请求无限阻塞 |
| MySQL 连接池 | **最大 64 / 空闲 16 / 连接寿命 1h** | `config/loadconfig.go:36-41`、`db/db.go:36-38` | 驱动层硬闸门 |
| 回源闸门 | **`MaxOpenConns/2` = 32 / 进程** | `dbguard/dbguard.go`、`db/db.go:43` | 随连接池自动伸缩，避免两边脱节 |
| Redis 连接池 | **64 / 空闲 8 / 池等待 200ms / 重试 1** | `middleware/redis/redis.go` | 池满快速失败，故障时不重试放大 |
| 异步回写并发 | **256 个槽位** | `video/video_cache.go:100` | 满了丢弃，下一轮 miss 会再补 |
| 热榜窗口 | **60 分钟桶** | `feed/service.go:33` | 滑动窗口长度 |
| 热榜桶 TTL | **2h** | `video/popularity_cache.go:15` | 必须大于 60 分钟合并窗口 |
| 热榜快照 TTL | **2min** | `feed/service.go:54` | 1 分钟封桶 + 1 分钟余量 |
| 热榜快照上限 | **5000** | `feed/service.go:71` | 把合并排序的 M 压成常数 |
| 热榜合并超时 | **500ms** | `feed/service.go:77` | ZUNIONSTORE 单独预算 |

---

# 一、第一层：基础原理（Q1-Q5）

## Q1. 为什么要设计本地缓存、Redis、MySQL 三级？只使用 Redis 不行吗？本地缓存带来了哪些额外问题？

**一句话结论**
三级不是「技术越多越好」，而是按「内存 → 网络 → 磁盘」的代价阶梯，把同一份数据的读按命中概率分层：L1 吸收进程内瞬时重复读，L2 吸收跨实例重复读，L3 只承担真正未命中的冷读。

**只使用 Redis 不行吗？**
可以跑，但有两个代价：

1. **热点 key 的集中压力**：热点视频的 key 只落在一个 Redis 节点 / 一个 slot 上。几千 QPS 全打同一个分片，带宽和单线程 CPU 都会成为瓶颈。L1 把绝大部分读留在本机内存。
2. **每次读都有固定开销**：一次 RTT（同机房约 0.2-0.5ms）+ 反序列化 + 连接池占用。一页 20 个 ID，用 `MGET` 是 1 次 RTT；但 QPS 上万时，这 1 次 RTT 也会变成可观的网络与连接数。

**L1 带来的额外问题（必须完整说出，不能只讲好处）**

| 问题 | 具体表现 | 本项目如何应对 |
|---|---|---|
| **多实例不一致** | 写路径只 `DEL` Redis，没有任何广播去清别的进程的 L1 | 把 L1 TTL 压到 5s，把不一致窗口显式限制住 |
| **内存不可控** | `go-cache` 没有容量上限、没有 LRU，只能靠 TTL 清 | 5s TTL 让存活集合较小；但理论上仍可被高基数请求撑大 |
| **冷启动击穿** | 进程重启 / 扩容后 L1 全空，压力瞬间到 L2 | L2 的 `MGET` + L3 的 singleflight 兜住 |
| **排障困难** | 同一请求打到不同 Pod，可能看到不同的点赞数 / 已删除视频 | 靠 TTL 收敛；日志里要能看出命中了哪一级 |
| **与强一致需求冲突** | 「我刚改完，必须立刻看到」这类字段不能进 L1 | 按字段判断，强一致读绕过 L1 |

**判断标准**
读放大高 + 能容忍秒级脏 + 数据基数可控 → 加 L1 值得；写多读少、强一致、字段频繁变更 → 只用 Redis 更简单。

**代码依据**
- 三级链路：`backend/internal/feed/service.go:123-263`
- L1 构造：`backend/internal/feed/service.go:118`
- 实体 key / TTL 收敛：`backend/internal/video/video_cache.go:24-34`

**可能追问：L1 为什么不做跨实例广播失效？**
因为成本与收益不匹配：Feed 对 5 秒旧数据不敏感；引入 Pub/Sub 后仍有消息丢失、Pod 重启漏消息的问题，最后还是需要 TTL 兜底。用 5 秒 TTL 换掉一套广播基础设施，是这个量级下的合理取舍。详见 Q15。

---

## Q2. 本地缓存 TTL 为什么是秒级？Redis 实体缓存为什么 1 小时、关注流列表为什么要分 30 秒和 24 小时？这些时间如何确定？

**一句话结论**
L1 的 TTL 是「跨实例脏数据窗口的上界」；L2 实体 TTL 是「写失效失败时的兜底上界」；列表 TTL 分两层：首页 30s 兜「新视频/取关」，深层页 24h 兜「昂贵的 JOIN 查询」。三者含义不同，不能用一个公式套。

**逐个拆**

| 层 | TTL | 它到底是什么 | 为什么是这个量级 |
|---|---|---|---|
| L1 | **5s** | 多实例不一致的最坏窗口 | 没有跨实例失效广播，TTL 直接等于「实例 A 看不到实例 B 的写」的最长时间。设长 → 用户看到旧点赞数 / 已删视频；设短 → 失去本地缓存意义。Feed 能忍几秒旧数据，5s 是折中 |
| L2 实体 | **1h** | 写路径 `DEL` 失败时的兜底上界 | 一致性主要靠事件驱动失效（点赞、删除都会 `DEL` + 版本 +1），TTL 只是「万一 DEL 失败，别永久脏」。而且实体是**读时按需写入**，每个 key 的过期时刻由首次访问决定，天然打散，不会整点一起失效 |
| L2 关注流首页 | **30s** | 新视频 / 取关变化的可见延迟上界 | 首页会被新内容影响，不能缓存太久；页内字段本来就每次 hydrate，所以 30s 只影响「哪些 ID 出现在第一页」 |
| L2 关注流深层页 | **24h** | 昂贵 JOIN 的复用窗口 | 游标单向翻页，已翻过的页不会被新视频影响；取关会按 pattern 失效。24h 是「查询贵 + 成员变化少」的折中 |
| 空值哨兵 | **60s** | 「确定不存在」这个判断的有效期 | 数据后来补上时，最多延迟 60s 就能读到；太长会把新视频误判为不存在 |
| 版本号 | **25h** | CAS 比较基准的存活期 | 必须明显长于实体 1h；版本先过期会让迟到的旧值重新通过校验 |
| 热榜快照 | **2min** | 一次分页会话的冻结窗口 | 必须 > 1 分钟：只有等 `asOf` 所在分钟桶封桶后再重建，结果才和首份快照逐位一致；2min = 1min 封桶 + 1min 余量 |
| 热榜分钟桶 | **2h** | 计数器存活期 | 必须 > 60 分钟的合并窗口，否则最老的桶在合并前就过期，热度会算少 |

**「如何确定」的方法（比记住数字更重要）**

1. 先确定一致性主机制：是**写失效**还是**纯 TTL**。
2. TTL 是兜底，不是主机制。上界 = 「主机制失败时，业务能容忍的最坏脏窗口」。
3. 下界 = 「回源成本 × 回源 QPS」可接受。TTL 越短，回源越多。
4. 最后用指标校准：各级命中率、回源 QPS、脏数据投诉。**本项目没有真实压测数据，这些值是工程估算，不是调参调出来的** —— 面试时要主动承认这一点。

**逐个追问**

- **为什么实体 1h 而不是 5min？**
  因为实体是热点共享数据，1h 能显著减少回源；一致性靠 `DEL` 保证，不靠 TTL。1h 只是「DEL 失败时别脏太久」的上界。
- **关注流为什么首页 30s、深层页 24h？**
  游标翻页是单向的：新视频只会改变第一页，深层页的内容集合基本不变，所以深层页可以缓存久。首页 30s 是为了让新发布和取关及时反映；即使成员旧了，页内内容仍会 hydrate，不会把点赞数/标题冻住。
- **为什么不做 TTL 随机抖动？**
  实体的过期时刻本来就被「首次访问时间」打散了；列表 key 带 `accountID + 游标`，也天然分散；快照 key 带分钟级 `asOf`。真正需要抖动的「大批 key 同时过期」在本项目不突出。**更重要的是：抖动对「Redis 整体不可用」这种雪崩完全无效**，详见 Q13。

**代码依据**
`feed/service.go:43,118`、`videocache/entity.go`、`video/popularity_cache.go:15-18`、`feed/service.go:48-77`。

---

## Q3. 什么是缓存穿透、击穿、雪崩？你的方案分别解决了哪些？

**一句话结论**
击穿是「一个热点 key 失效，瞬间并发回源」；穿透是「查一个根本不存在的数据，每次都不命中」；雪崩是「大量 key 同时失效，或整个缓存层不可用」。三者成因不同，解法不能互换。

**对照表**

| | 缓存击穿 | 缓存穿透 | 缓存雪崩 |
|---|---|---|---|
| 触发条件 | **单个热点 key** 过期 / 被删 | 查询的 key **在数据源里也不存在** | 大量 key 同时过期，**或 Redis 整体不可用** |
| 本项目手段 | 实体：singleflight；关注流：SETNX；L1 也算一层 | `__null__` 哨兵，TTL 60s | 连接池上限 + `dbguard`；回写版本 CAS；TTL 天然分散 |
| 代码 | `feed/service.go` 实体回源、`ListByFollowing` | `videocache/entity.go`、`feed/service.go` `GetVideoByIDs`、`video/video_service.go` `GetDetail` | `db/db.go`、`dbguard/dbguard.go`、`videocache/entity.go` |
| 残余风险 | singleflight 只到 **P（Pod 数）**；SETNX 轮询超时后会兜底查库 | 恶意枚举会多出一批短 TTL 占位 key；防攻击要靠网关限流 | 闸门是**每进程**的；熔断、全局限流、指标都没有 |

**必须分清的两个「雪崩成因」**

| 成因 | 解法 | 本项目 |
|---|---|---|
| (a) 大量 key **同时过期** | TTL 随机抖动 + 多级缓存 + 逻辑过期 | 没有主动抖动；靠「读时按需写入 + key 带分钟/用户维度」天然打散 |
| (b) 缓存层**整体不可用** | 熔断 + 限流 + 降级 + DB 保护 | 全量降级 + 连接池上限 + 统一回源闸门；**没有熔断** |

> **最容易答错的一句**：「雪崩就用 TTL 随机抖动」。抖动只对成因 (a) 有效；Redis 都挂了，key 过没过期根本不重要。

**本项目击穿防护的层次**

1. **L1（5s）**：同一实例的瞬时重复读在本地就被吸收，压力到不了 L2。
2. **singleflight（实体）**：同一进程内，同 key 的并发回源合并成 1 次。N → P（每个 Pod 1 次），不是 N → 1。
3. **SETNX（关注流列表页）**：跨进程互斥，N → 1（理想）；没抢到锁的请求轮询缓存，等不到才兜底查库。

**穿透为什么用哨兵而不是布隆过滤器**

| | 布隆过滤器 | 空值哨兵 |
|---|---|---|
| 新发布视频 | **可能误判为不存在 → 404**（只保证插入过的查得到） | 该 ID 不在占位里，正常查库命中 |
| 删视频 | 标准 BF 不支持删除，已删 ID 永久残留，误判率持续上升 | TTL 到期自动重查 |
| 多实例 | 每个 Pod 一份内存 BF，发布要广播同步 | 都在 Redis，天然一致 |
| 复杂度 | 预热任务 + 误判率调参 + 新依赖 | 几行代码 |

对一个自增 videoID 的短视频项目，**把新视频误判成 404** 是功能错误，不是性能问题，演示时极难发现 —— 这是不用布隆过滤器的核心理由。

**哨兵的三个坑（能说出细节 = 真写过）**

1. 哨兵不能用 JSON 的 `"null"`：`json.Unmarshal([]byte("null"), &v)` **不报错**，只会留下零值，缓存出一个 `ID=0` 的垃圾实体。
2. 必须在 `json.Unmarshal` **之前**识别哨兵，否则非法 JSON 解析失败 → 掉进 `missedL2` → 照样查库，等于没做。
3. TTL 必须短：60s。数据后来补上时，最多延迟这么久。

**雪崩防护的残余缺口（主动说）**

- `dbguard` 是**每进程**的，容量 = `MaxOpenConns/2`（默认 32）；10 个 Pod 就是 320 个并发回源；MySQL 能不能扛住取决于机器和池配置，不是代码能保证的。
- 闸门已覆盖全部 Feed DB 入口（实体、冷区、关注流、热榜兜底、话题标签、话题流、`GetDetail`）；`IsLiked` 在闸门满时降级为 `false`。
- 没有熔断：DB 变慢时不会快速失败，只会把连接池和 goroutine 堆满。
- 没有全局限流：HTTP 层的限流是 Redis 计数，**Redis 挂了它直接放行**，而且 Feed 读接口根本没挂限流。

---

## Q4. singleflight 的作用是什么？它与互斥锁有什么区别？为什么不直接用 sync.Mutex？

**一句话结论**
singleflight 是「按 key 合并**并发读**」，把 N 个相同请求折叠成 1 次执行，并把 leader 的返回值**直接共享**给所有等待者；`sync.Mutex` 是「按临界区串行化」，等待者拿到锁之后仍然要自己执行一遍逻辑。

**对比表**

| | singleflight | `sync.Mutex` | Redis SETNX |
|---|---|---|---|
| 作用域 | 单进程，按 key | 单进程，按锁对象 | 跨进程 |
| 等待者做什么 | 阻塞，**直接拿 leader 的返回值** | 排队，拿到锁后**自己再执行** | 轮询缓存，等不到自己查库兜底 |
| 能否合并不同 key | 能，key 不同互不影响 | 不能，同一把锁会串行化无关请求 | 能，锁 key 不同互不影响 |
| 结果共享 | **是**（同一个指针） | 否 | 否（只能通过缓存共享） |
| 缓存依赖 | 不依赖，leader 失败 follower 也拿到错误 | 不依赖 | 强依赖 Redis |
| 超时/取消 | `Do` 不接收 ctx（要用 `DoChan`） | 自己控制 | ctx 控制加锁 |
| 典型用途 | 热点读回源合并 | 保护多步状态变更 / 写路径 | 跨实例重建互斥 |

**为什么不用 `sync.Mutex`？**

假设给每个 videoID 配一把锁：

```text
100 个请求同时 miss videoID=123
  → 第 1 个拿到锁，查库，写缓存，解锁
  → 第 2 个拿到锁，如果它不 double-check 缓存，就再查一次库
  → 第 3 个……第 100 个
```

要用 mutex 达到 singleflight 的效果，必须：

1. 每个 ID 一把锁（全局锁会把无关视频串行化）；
2. 拿到锁后先 double-check 缓存（否则每个人还是要查库）；
3. 自己维护 `map[uint]*sync.Mutex` 并负责清理（否则 key 越积越多，内存泄漏）；
4. 自己把结果传给等待者（mutex 本身不共享返回值）。

这四步做完，你其实重新实现了一个更慢、更容易出 bug 的 singleflight。singleflight 的 `Group` 内部就是一个 `map[string]*call`，调用结束自动删除条目，不需要自己管理生命周期。

**singleflight 的代价与坑**

- **只合并「重叠」的调用**：leader 完成后，新来的请求会开启新的一轮，不会记住上次结果。它不是缓存。
- **只到 P，不到 1**：状态在进程内存，跨 Pod 无效。这是 Q13 追问的核心。
- **共享指针**：leader 返回的指针会被所有 follower 拿到，必须 `safeCopy`，否则一个请求写 `IsLiked` 会污染其他用户。
- **`Do` 不接收 ctx**：leader 卡住时所有 follower 永久阻塞；follower 的客户端断开也无法退出。改进方向是 `DoChan` + `select{ctx.Done(), result}`。
- **leader 的 ctx 会连累所有人**：本项目闭包里用的是 leader 请求的 `ctx`；leader 的客户端断开 → ctx 取消 → 查库失败 → 999 个 follower 一起拿到错误。严格说，共享回源应该用 `context.WithoutCancel(ctx)` + 独立超时，而不是绑定某一个请求的生命周期。

**代码依据**
`feed/service.go:235-263`、`video/video_cache.go:67-93`、`feed/service.go:256,284`（两处 safeCopy）。

---

## Q5. Redis 的 MGET 能解决什么问题？与循环 GET 相比有哪些性能优势？

**一句话结论**
`MGET` 把 N 次「客户端 → Redis → 客户端」的往返压成 1 次，代价是「全有或全无」：命令报错时拿不到任何一个 key 的结果，所以降级也只能是全量降级。

**性能账（一页 20 个 ID）**

| | 20 次 GET | 1 次 MGET |
|---|---|---|
| 网络往返 | 20 次 | **1 次** |
| 假设单次 RTT 0.3ms | ≈ 6ms | ≈ 0.3ms |
| 客户端 syscall / 协议解析 | 20 次 | 1 次 |
| 部分失败隔离 | 只影响失败的那几个 | 整批一起失败 |
| Redis 服务端 CPU | O(20) | O(20)（差不多，Redis 单线程逐 key 处理） |

省下来的是**往返和客户端开销**，不是 Redis 服务端的 CPU。所以 QPS 越高、RTT 越大，`MGET` 的优势越明显。

**MGET 的语义**

```redis
MGET video:entity:1 video:entity:2 video:entity:3
1) "{\"ID\":1,...}"
2) (nil)                  ← key 不存在，不是错误
3) "{\"ID\":3,...}"
```

- 返回数组长度**永远等于**传入 key 的数量。
- `nil` 表示「这个 key 不存在」，不是命令失败。
- `err == nil` 只说明「命令执行成功」，不代表「所有 key 都命中」。
- 类型断言失败、JSON 解析失败也应当作 miss 处理，而不是让请求报错。

**MGET 的代价**

1. **整体成功 / 整体失败**：连接错误、超时、集群 `CROSSSLOT`，任一发生都拿不到部分结果。所以 `missedL2 = missedL1` 是全量降级，不是「部分降级」。
2. **大 N 会阻塞 Redis**：`MGET` 是 O(N)，一次传 1000 个 key 会长时间占用单线程。生产上建议每批 50-100 个。
3. **Redis Cluster 的坑**：不同 slot 的 key 不能放在同一个 `MGET`，否则报 `CROSSSLOT`。本项目是单实例（docker-compose 一个 redis），所以没这个问题；上集群后要么加 hash tag，要么按 slot 分组。

**Pipeline 是折中方案**
Pipeline 也是 1 次 RTT，但能拿到逐条结果和逐条错误，不会「一个坏 key 拖垮整批」。代价是客户端要缓存 N 条命令、解析 N 条回复，复杂度比 `MGET` 高。本项目没做，所以选择了「简单 + 故障时全量降级」。

**代码依据**
`feed/service.go:160-191`；nil / 类型错 / JSON 坏的三种 miss 处理在 `164-185`。

---

# 二、第二层：实现细节（Q6-Q12）

## Q6. 假设 1000 个 Goroutine 同时查询同一个过期的热点视频，完整调用链是什么？MySQL 最终收到多少次查询？

**先给结论**

- **同一个 Pod 内**：MySQL 收到 **1 次**查询。
- **P 个 Pod 上各有 1000 个并发**：MySQL 收到 **P 次**（每个 Pod 各 1 次），不是 1 次。
- **Redis 收到的是 1000 次 `MGET`**：singleflight 只在 L3 合并，不合并 L2 请求。

**单 Pod 的完整链路**

```text
1000 个 goroutine 同时进入 GetVideoByIDs([123])
  │
  ├─ L1：1000 次 go-cache.Get，全部 miss（假设 key 刚过期）
  │
  ├─ L2：1000 次 MGET（每个请求一次，互不合并）
  │      Redis 返回 1000 个 [nil]
  │      → 1000 个请求都进入 missedL2
  │
  ├─ L3：每个请求起 1 个 goroutine 去查 123
  │      1000 个 goroutine 都调用 EntityLoadGroup.Do("sf:entity:123", fn)
  │        ├─ 第 1 个成为 leader：抢回源闸门 → SELECT ... WHERE id=123
  │        └─ 其余 999 个阻塞在 Do 内部，不执行 fn
  │      leader 返回 → 999 个 follower 拿到同一个 *Video
  │
  ├─ 回写：
  │      leader：有界异步回写，版本 CAS 成功才 SET(video:entity:123, 1h)
  │      每个请求：同步写自己的 L1(5s)                ← 1000 次本地写
  │
  └─ buildOrderedResult：按原顺序返回
```

**为什么 Redis 还是收到 1000 次 MGET？**
因为 `MGET` 在 singleflight 之前。singleflight 只包住 L3 的 MySQL 查询，不包住 L1/L2。要合并 L2，需要把 singleflight 提前到「整个读路径」，但那时 key 得是「整页 ID 集合」，不同页很难合并，收益和复杂度要重新算。

**如果 Redis 也挂了会怎样？**

```text
1000 个 MGET 全部报错 / 超时
  → missedL2 = missedL1
  → 1000 个 goroutine 全部走到 L3
  → singleflight 仍然把同 key 合并成 1 次
  → 但这 1 次要先抢 dbguard（容量 MaxOpenConns/2）
  → 单个热点 key 只占 1 个配额，所以闸门不会挡住它
  → 最终 MySQL 仍是 1 次（本 Pod）
```

**跨 Pod 的真实数字**

| 场景 | MySQL 查询次数 |
|---|---|
| 1 个 Pod，1000 并发，同一个视频 | **1** |
| 10 个 Pod，每个 100 并发，同一个视频 | **10** |
| 10 个 Pod，每个 100 并发，一页 20 个热门视频 | 最多 **200**（每 Pod 每个视频 1 次） |
| leader 查库失败 | 1 次失败 + follower 全部拿到错误；客户端重试后重新选 leader |
| 闸门已满 | 0 次查询；该请求返回 `ErrServiceOverloaded` → 503（不再返回缺条的 200） |

**这道题真正考的三个点**

1. 你知道 singleflight 在**哪一层**（L3），不在 L2。
2. 你知道它是**进程内**的，所以是 N → P，不是 N → 1。
3. 你知道 **leader 的 ctx 会连累 follower**：本项目闭包用的是 leader 请求的 ctx，leader 客户端断开会导致所有 follower 一起失败。

**代码依据**
`feed/service.go` 的 `GetVideoByIDs`；singleflight key 为 `sf:entity:<id>`；闸门为 `dbguard.Do`；L1 写入在每个请求的 goroutine 内。

---

## Q7. singleflight.Group 应该按什么粒度使用？所有视频共用一个 key、每个视频独立 key，还是每类业务一个 Group？

**一句话结论**
**每类「回源结果可互换」的数据源一个 Group，key 里带足区分维度。** 不是所有视频共用一个 key，也不是每个视频建一个 Group。

**三种方案的取舍**

| 方案 | 是否可行 | 问题 |
|---|---|---|
| 所有视频共用一个 key（如 `"video"`） | 绝对不行 | 100 个不同视频会被合并成 1 次查询，返回同一个结果，数据串号 |
| 每个视频一个 `singleflight.Group` | 能跑，但没必要 | `Group` 本身就是并发安全的 map，按 key 区分即可；每个视频建 Group 浪费对象，而且无法让 Feed 和详情页共享同一次回源 |
| **每类业务一个 Group，key 带实体 ID** | **正确** | 需要保证 key 不串业务、不串维度 |

**本项目的实际划分**

| Group | key | 用途 | 为什么这样分 |
|---|---|---|---|
| `video.EntityLoadGroup` | `sf:entity:<id>` | 视频实体回源，Feed + 详情页共用 | 两条路径查的是同一张表、同一行、同一个 key，结果可以互换。共用 Group 才能把「Feed 请求」和「详情页请求」合并成一次查库 |
| `FeedService.requestGroup` | `sf:fallback:global_timeline_rebuild` | ZSET 为空时重建 1000 条时间线 | **全局唯一**：所有用户、所有游标一起重建，结果可互换 |
| `FeedService.requestGroup` | `sf:cold:listLatest:<limit>:<before>:<id>` | 冷区分页 | **参数化**：不同用户/游标的结果不能互换，key 必须带全维度 |
| `FeedService.requestGroup` | `sf:stitch:listLatest:<remain>:<before>:<id>` | 冷热边界补页 | 同上 |
| `FeedService.requestGroup` | `sf:hot:merge:<asOf>` | 热榜快照重建 | 按 `asOf` 参数化；同一分钟的快照可互换 |

**key 粒度的判据**

> **同一次回源的结果，能不能原样给另一个请求？**
> 能 → 可以共用 key；不能 → key 必须包含「让结果不同的所有维度」。

- 实体：结果只由 `videoID` 决定 → key = ID。
- 冷区分页：结果由 `limit + 游标` 决定 → key 必须带这三个。
- 时间线重建：结果是「最近 1000 条」，与用户无关 → 用全局 key。
- 热榜快照：结果由 `asOf` 决定 → key 带 `asOf`。

**为什么 Feed 和详情页要共用一个 Group？**
这是修复过的一个真实问题：以前两条路径各有一套防击穿机制（Feed 用 singleflight，详情页用 SETNX），**并联且互不感知**，同一瞬间同一视频仍有 P+1 次回源。收敛到一个 Group 后，两条路径才能真正合并。详见 `面试/Feed三级缓存与缓存自愈-面试专题.md` 的 F5。

**要不要担心 Group 的锁竞争？**
`singleflight.Group` 内部是一把 `sync.Mutex` 保护整个 map。极高 QPS、大量不同 key 时会有锁竞争，但临界区很短（只是查/插 map），通常不是瓶颈。真到瓶颈再按 key hash 分片成多个 Group，不要提前优化。

**代码依据**
`video/video_cache.go:67-93`、`feed/service.go:250-263`（异步回写）、`videocache/entity.go:81`（失效）、`feed/service.go:706-715`（关注流加锁）。

---

## Q8. 如果 singleflight 合并了请求，但查询结果没有写入缓存，下一个请求会发生什么？

**一句话结论**
**当前这一批请求没事，下一批重新查库。** singleflight 只合并「同时发生」的调用，不保存结果；「合并」和「缓存」是两件独立的事。

**分场景看**

| 场景 | 当前这批请求 | 同一 Pod 的后续请求 | 其他 Pod |
|---|---|---|---|
| leader 查到结果，**Redis 写失败** | 正常返回（共享 leader 的结果） | 5s 内 L1 命中；5s 后重新回源 | 各自回源 |
| leader 查到结果，**L1 写也失败**（几乎不会） | 正常返回 | 下一次立刻回源 | 各自回源 |
| leader **查库报错** | 所有 follower 一起拿到错误 | 下一次重新选 leader | 各自重试 |
| leader **ctx 被取消**（客户端断开） | 所有 follower 一起失败 | 下一次重新选 leader | 各自重试 |
| leader **panic** | 等待者会收到 panic / `errGoexit`（取决于 x/sync 版本） | 下一次重新选 leader | 各自重试 |
| 进程崩溃 | 当前请求失败 | 重启后 L1 全空 | 不受影响 |

**为什么同一 Pod 还能撑 5 秒？**
因为第 289 行每个请求都会把结果写进自己的 L1：

```go
f.localcache.Set(video.VideoEntityKey(f.rediscache, safeCopy.ID), safeCopy, 5*time.Second)
```

即使 leader 的 Redis 回写失败，同一 Pod 的后续请求在 5 秒内仍能从 L1 拿到。**这是 L1 的一个隐性价值：它是 Redis 回写失败时的第二道缓冲。**

**关键认知：singleflight 不提供「记住结果」的能力**

- 它不是缓存，没有 TTL，没有 key-value 存储。
- 它只保证「同一时刻、同一个 key、只有一个 fn 在执行」。
- 高 QPS 下，leader 完成后新来的请求会开启新一轮；如果缓存一直写不进去，**DB QPS ≈ 请求到达速率**，而不是被 singleflight 压成常数。singleflight 只能压「并发」，不能压「总量」。

**现在还剩下的缺口：回写失败/丢弃不可观测**
回写已经改成有界 + 版本 CAS，失败会打日志：

```go
if !video.ScheduleEntityWrite(func() {
    setCtx, setCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
    defer setCancel()
    video.SetVideoEntityIfVersion(setCtx, f.rediscache, id, b, video.VideoEntityTTL, version)
}) {
    // 槽位已满：本次回写被丢弃
}
```

`SetVideoEntityIfVersion` 会记录 Redis 错误；但「槽位满导致丢弃」目前没有指标，也没有重试。影响是下一次 miss 要重新查库，不影响正确性。改进方向：加 `cache_write_dropped_total` / `cache_write_failed_total` 计数，并在持续丢弃时告警。

**代码依据**
`feed/service.go` 的 `GetVideoByIDs`（回写与 L1 写入）、`video/video_cache.go` 的 `ScheduleEntityWrite` / `SetVideoEntityIfVersion`。

---

## Q9. 本地缓存如何实现？用 sync.Map、互斥锁 map 还是现成缓存库？如何处理并发过期和内存淘汰？

**一句话结论**
本项目用 `patrickmn/go-cache`：内部是 `map[string]Item` + `sync.RWMutex` + 每条目过期时间 + 后台 janitor。它解决「并发安全 + TTL + 定期清理」，但**不解决容量上限和淘汰**。

**go-cache 的内部结构**

```text
Cache
 ├─ mu        sync.RWMutex
 ├─ items     map[string]Item
 ├─ defaultExpiration
 └─ janitor   *janitor（每 cleanupInterval 执行一次 DeleteExpired）

Item
 ├─ Object     interface{}
 └─ Expiration int64（UnixNano，0 表示永不过期）
```

- `Set(k, v, d)`：写 map，`Expiration = now + d`。
- `Get(k)`：加读锁，取出 Item，判断 `now > Expiration`；过期就当作不存在（懒删除，不立即删）。
- `DeleteExpired()`：加写锁，遍历整个 map，删掉已过期的条目。
- `cache.New(3s, 5s)`：默认 TTL 3s；janitor 每 5s 扫一次。

**并发过期怎么处理？**

- 读的时候**懒判断**：`Get` 发现过期就返回 miss，不会把过期值返回。
- 后台**定期清理**：janitor 每 5s 扫一遍，把过期条目真正从 map 删掉。
- 两者都在锁内执行，不存在并发读写 map 的 panic。
- 副作用：过期条目在物理上可能多存活最多 5s（janitor 间隔），所以内存占用 ≈ 2 倍 TTL 窗口的活跃集合。

**内存淘汰怎么处理？**

**没有淘汰。** go-cache 只有 TTL，没有 LRU/LFU，也没有容量上限。这意味着：

- 如果 5 秒内被访问的 videoID 基数很大，map 就会涨到那么大；
- 恶意请求大量不同 ID，可以把 L1 撑大（虽然 Feed 每页最多 50 个 ID，但仍可构造）；
- 没有「内存到阈值就淘汰最久未使用」的保护。

**三种实现对比**

| 方案 | TTL | 容量上限 | 淘汰 | 并发 | 适用 |
|---|---|---|---|---|---|
| `sync.Map` | 无，需自己加时间戳 | 无 | 无 | 读多写少友好 | key 集合稳定、自己实现 janitor |
| mutex + map | 无，需自己实现 | 无 | 无 | 一把锁，写会阻塞读 | 最简单、量小 |
| **go-cache（本项目）** | 内置 | 无 | 无 | RWMutex | 想要 TTL、量可控 |
| ristretto | 有 | 有 | TinyLFU 准入 + 采样淘汰 | 高并发优化 | 命中率敏感、内存要可控 |
| bigcache / freecache | 有 | 有 | 分片 / 环形缓冲 | 降低 GC 压力 | 大容量、value 是字节 |

**如果要把 L1 做稳，至少补三件事**

1. **容量上限**：超过 N 条就拒绝写入或淘汰最旧的；防止基数爆炸。
2. **命中率/条目数指标**：否则你不知道 L1 到底有没有用。
3. **并发回源保护**：L1 过期瞬间，同一 Pod 的所有请求会一起涌向 L2；如果 L2 也 miss，就靠 L3 的 singleflight 兜。

**一个实现细节**
L1 里存的是 `video.Video` **值**，不是指针：

```go
f.localcache.Set(key, safeCopy, 5*time.Second)  // safeCopy 是值
```

取出来时 `v.(video.Video)` 再取地址，每个请求拿到的是自己的副本。这避免了「一个请求改缓存对象，污染所有请求」的问题。如果塞指针进去，必须保证下游只读。

**代码依据**
`feed/service.go:118,134-157,178,250`。

---

## Q10. Redis 实体缓存与列表缓存是什么关系？视频点赞数变化时要删哪些缓存？如何避免旧数据重新回填？

**一句话结论**
实体缓存是「一份共享的视频内容」；关注流的列表缓存现在只存「这一页有哪些 ID、游标是什么」，**不再嵌实体内容**。所以点赞、改名、删视频只需要失效实体，列表缓存不必跟着清 —— 命中列表后仍会走 `GetVideoByIDs` 重新装配。

**两者对比**

| | 实体缓存 | 列表缓存（关注流） |
|---|---|---|
| key | `video:entity:<id>` | `feed:listByFollowing:v2:limit=<n>:accountID=<id>:before=<ms>:id=<id>` |
| 内容 | 一条视频的完整字段 | `VideoIDs []uint` + `NextTime` + `NextIDBefore` + `HasMore` |
| 维度 | 全站共享，一份 | 每人每页一份 |
| 谁读 | 最新流、热榜、详情页、关注流 hydrate | 关注流 |
| TTL | 1h | 首页 30s；深层页 24h |
| 失效 | 点赞/评论/删除时 `DEL` + 版本 +1 | 关注/取关时按 pattern 删 |

**点赞数变化时，要删哪些？**

1. `video:entity:<id>`：**已做**。`UpdatePopularityCache` 第一件事就是 `InvalidateVideoEntity`。
2. `feed:listByFollowing:v2:*`：**不需要**。列表里没有 `LikesCount` / `IsLiked`，下次 hydrate 会从实体或 DB 重新装配。
3. 热榜 / 最新流：**不需要**。它们只缓存 ID 顺序（ZSET）和实体，内容每次请求都 hydrate；实体删了就会重新装配。

**现在的实际表现**

| 场景 | 实际表现 |
|---|---|
| 用户点赞后刷关注流 | 实体被失效；下一次 hydrate 回源拿到新 `LikesCount`；`IsLiked` 本来就每页现查 |
| 作者改标题 | 同上，列表不再冻结旧标题 |
| 作者删视频 | 删除时失效实体并移出热时间线；关注流 hydrate 查不到该 ID，最多受 L1 5s 影响后消失 |
| 最新流 / 热榜 | 实体被删后下次读会回源 |

> 这里的关键不是「多加了一次 MGET」，而是**把「因用户而异的字段」和「全站共享的内容」分开了**。列表缓存只回答「这一页是谁、下一页从哪开始」，不再回答「这一页长什么样」。

**为什么选「只存 ID」而不是「给整页缓存补失效」？**

| 方案 | 做法 | 代价 |
|---|---|---|
| A. 补失效 | 视频变更时 `DelByPattern("feed:listByFollowing:*")` | 改一个视频 → 清空**所有人**的关注流缓存，写放大极大 |
| B. 改结构（已采用） | 列表只存 ID + 游标，内容走实体缓存 | 多一次 `MGET`；`IsLiked` 仍要每页查（本来就因人而异） |

失效粒度从「全站」变成「单个实体」，可控得多。`IsLiked` 仍然无法跨用户共享，因为它是 viewer 维度的。

**一个仍然存在的结构问题：实体缓存里混了高频变化字段**

`video.Video` 同时包含：

- 低频变化：`Title`、`Description`、`PlayURL`、`CoverURL`、`CreateTime`；
- 高频变化：`LikesCount`、`Popularity`。

而每次点赞/评论都会 `DEL` 整个实体。结果是：**越热的视频，实体缓存被删得越频繁，命中率越低** —— 这恰好和「热点数据应该缓存得更好」相反。当前靠 L1(5s) + singleflight 扛住，但这是一个结构性反模式。

更合理的设计是拆分：

- 不可变内容缓存 1h；
- 点赞数 / 热度单独用 Redis 计数器（或短 TTL 的小 key），读时合并；
- 这样点赞只更新计数器，不会把整条实体缓存打掉。

**如何避免旧数据重新回填？**
已实现「版本号 + Lua CAS」，见 Q14。读路径在查库前批量读取版本号，回写时版本变了就丢弃；版本号读不到（Redis 故障）则本次不回写。

**代码依据**
`feed/entity.go`（`FeedVideoItem`）、`feed/service.go:106`（`CachedFollowingPage`）、`feed/service.go:595-755`（关注流 hydrate / 缓存）、`video/video_cache.go:58`（`InvalidateVideoEntity`）、`social/service.go`（关注流失效）、`videocache/entity.go`（key / TTL / CAS）。

---

## Q11. MGET 查询 100 个视频时，其中 20 个未命中，如何组织 MySQL 批量查询和结果回填？

**先看本项目现在怎么做**

**20 个 goroutine，每个一次单行查询，不做批量。**

```text
missedL2 = [20 个 ID]
  for each id:
      go func(id) {
          EntityLoadGroup.Do("sf:entity:"+id, func() {
              抢闸门
              repo.GetByIDs(ctx, []uint{id})   // ← 20 条 SELECT ... WHERE id = ?
              ...
          })
      }(id)
  wg.Wait()
```

**为什么不做 `WHERE id IN (...)`？**
为了配合 singleflight 的「按 ID 合并」：
- 如果批量查，singleflight 的 key 就得是「整批 ID 的组合」。两个请求的 ID 集合只要差一个，就无法合并；
- 按 ID 单查，key 是 `sf:entity:<id>`，不同请求只要撞到同一个视频就能合并 —— 热点视频的合并率最高。

**代价**
一页 20 个冷 ID 会产生 20 条单行 SQL。冷启动 / 缓存全失效时，这会明显放大 DB 的 QPS。

**如果要改成批量，怎么做才不丢合并能力？**

核心矛盾：**批量减少 SQL 条数，但会削弱跨请求的按 ID 合并**。有三种折中：

### 方案 A：先批量查，再按 ID 分发（简单，但合并率下降）

```go
// 1. 一次批量查回所有 miss 的 ID
rows, err := repo.GetByIDs(ctx, missedL2)     // 1 条 SQL
found := make(map[uint]*video.Video, len(rows))
for _, v := range rows { found[v.ID] = v }

// 2. 命中的：写 Redis / L1，直接填充结果
// 3. 没命中的：写 __null__ 哨兵，不回源
// 4. 仍然保留 singleflight，但闭包里优先用 found 里的值
for _, id := range missedL2 {
    go func(id uint) {
        v, _, _ := EntityLoadGroup.Do(key(id), func() (any, error) {
            if v, ok := found[id]; ok { return v, nil }   // 批量已查到，不再查库
            return nil, nil                               // 批量也没有 → 哨兵
        })
        ...
    }(id)
}
```

- 优点：每页 1 条 SQL，回填也只需 1 次 `MSET`。
- 缺点：并发请求之间不再按 ID 合并（每个请求都做一次批量查），DB 的 QPS 可能反而更高。
- 适用：**冷启动 / 低并发**，或者 miss 率很高时。

### 方案 B：批量 + 局部 singleflight（复杂，但两者兼得）

维护一个进程内的「in-flight ID 集合」：

1. 先从 in-flight 集合里排除掉「已经有人在查」的 ID；
2. 对剩下的 ID 做一次批量查；
3. 把结果发布给等待者（每个 ID 一个 promise / `sync.Once` + channel）；
4. 新来的请求如果发现 ID 在 in-flight，就等 promise，不参与批量查。

这就是「按页批量 + 按 ID 合并」的融合，代码量明显上升。本项目没做。

### 方案 C：保持单查，但只对「真正的冷 ID」批量

先批量查一次，只把「批量也没查到」的 ID 交给 singleflight 做二次确认（防止并发窗口内新插入），命中的直接回填。这样正常路径 1 条 SQL，异常路径才单查。

**回填时要注意的点**

1. **顺序**：批量查询返回的顺序不保证与入参一致，必须用 `map[id]*Video` 再按 `videoIDs` 重排（本项目已有 `buildOrderedResult`）。
2. **空值**：批量查不到的 ID 要写 `__null__` 哨兵，否则这 20 个里真正不存在的那些每次都会再查。
3. **`IN` 列表大小**：100 个 ID 没问题；上千个要分批（每批 100-500），并注意 `max_allowed_packet`。
4. **回写方式**：Redis 用 `MSET` / pipeline 一次写完，不要 20 次 `SET`。
5. **不要跨 singleflight key 批量**：批量结果必须能按 ID 拆开，否则合并语义就乱了。

**面试怎么答**
先承认现状：「我们现在是 20 个 goroutine 单查，为了按 ID 合并」。再给出判断：「主键单行查询本身很便宜，20 条并发查的延迟和 1 条批量差不多；真正的差别是 DB 的 QPS。如果压测显示这里是瓶颈，我会改成先批量查、再按 ID 分发，同时保留 singleflight 作为二次保护」。最后补一句取舍：「批量和合并是矛盾的，选哪个取决于 miss 率和并发形态，不能拍脑袋」。

**代码依据**
`feed/service.go:235-263`、`feed/repo.go:94-107`（`GetByIDs` 本身支持批量，只是调用方传了单元素）。

---

## Q12. Go Redis 客户端的超时、连接池、并发限制如何配置？Redis 超时后降级 MySQL 如何避免流量突然放大？

**一句话结论**
客户端已经显式配置了「超时 + 连接池 + 有限重试 + `ContextTimeoutEnabled`」，50ms 的 ctx 现在是真正生效的 socket 上限；降级路径靠 L1、singleflight、`dbguard` 和连接池四道缓冲，且闸门容量跟着连接池自动伸缩。

### 12.1 当前配置是什么

```go
rdb := redis.NewClient(&redis.Options{
    Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
    Password: cfg.Password,
    DB:       cfg.DB,

    ContextTimeoutEnabled: true,   // 50ms ctx 真正作用到 socket

    PoolSize:     cfg.PoolSize,     // 默认 64，可用 REDIS_POOL_SIZE 覆盖
    MinIdleConns: cfg.MinIdleConns, // 默认 8
    PoolTimeout:  200 * time.Millisecond,

    DialTimeout:  time.Second,
    ReadTimeout:   time.Second,     // 无 deadline 调用的兜底
    WriteTimeout: time.Second,
    MaxRetries:   1,
    ConnMaxIdleTime: 5 * time.Minute,
})
```

`backend/internal/middleware/redis/redis.go`。池大小与环境变量在 `config/loadconfig.go`（`REDIS_POOL_SIZE`、`REDIS_MIN_IDLE_CONNS`）。

### 12.2 为什么 `ContextTimeoutEnabled` 是关键

旧实现只配了 `Addr/Password/DB`，而 go-redis v9 默认 `ContextTimeoutEnabled=false`：传给 socket 的 ctx 会被替换成 `context.Background()`，于是上层 `WithTimeout(50ms)` 只剩「等连接」这一步受约束，**socket 读仍受 `ReadTimeout`（默认 3s）约束**。

```go
// go-redis v9, redis.go
func (c *baseClient) context(ctx context.Context) context.Context {
    if c.opt.ContextTimeoutEnabled {
        return ctx
    }
    return context.Background()
}
```

后果是 Redis 抖动时每个请求最多卡 3s，缓存从「加速层」变成「故障放大器」。修复只需要一个开关：

```go
ContextTimeoutEnabled: true
```

现在 50ms 是真实上限；同时保留 1s 的 `ReadTimeout` 作为「调用方没有传 deadline」时的兜底，避免无限等待。

> 面试时这段仍然值得讲：不是背「我们配了 50ms」，而是能说清「为什么默认配置下 50ms 是假的、开关在哪、修完之后的实际上限是多少」。

### 12.3 参数为什么这么定

| 参数 | 当前值 | 依据 |
|---|---|---|
| `ContextTimeoutEnabled` | true | 让上层 50ms 成为真实 socket 上限 |
| `PoolSize` | 64 | 按 Pod 数与 Redis `maxclients` 反推；默认值 10×GOMAXPROCS 不可控 |
| `MinIdleConns` | 8 | 避免冷启动时集中建连 |
| `PoolTimeout` | 200ms | 池满快速失败，不让请求排队 4s |
| `DialTimeout` | 1s | 建连失败要快速暴露 |
| `ReadTimeout` / `WriteTimeout` | 1s | 无 deadline 调用的兜底；比 50ms 宽，留正常抖动余量 |
| `MaxRetries` | 1 | 缓存读重试收益低，故障时重试会放大流量 |
| `ConnMaxIdleTime` | 5min | 及时释放空闲连接，适应 Redis 侧超时/扩缩容 |

要点：

1. **两级超时**：业务显式传 50ms 的 ctx，是「期望预算」；1s 读写超时是「没有预算时的安全网」。
2. **池大小按 Pod 配置**：总连接数 = Pod 数 × PoolSize，不能超过 Redis 的 `maxclients`。
3. **重试只留 1 次**：Redis 通常是本地/同可用区，重试多次只会在故障时放大流量。

### 12.4 并发限制

- **池大小**是硬限制：池满后请求在 `PoolTimeout` 内等连接，等不到就报错。
- **`dbguard`** 限制同时回源 MySQL 的请求数，间接限制「Redis 全部失败时的下游压力」。
- **异步回写有界（已做）**：`video.ScheduleEntityWrite` 用 256 个槽位的 channel 限制在途回写，满了直接丢弃并返回 `false`。回写只是加速下一次读，丢了不影响正确性。
- **没有额外加 Redis 信号量**：池本身已经是硬限制，再套一层只会增加复杂度；真正需要防的是「Redis 失败后涌向 DB」，那由 `dbguard` 负责。

### 12.5 Redis 超时后降级，如何避免流量突然放大

**本项目已有的四道缓冲**

1. **L1（5s）**：同一实例的重复读不进 Redis，也不进 DB。
2. **singleflight**：同一进程内同 key 的并发回源合并成 1 次。
3. **`dbguard`（`MaxOpenConns/2`，非阻塞）**：拿不到配额直接返回过载，不排队。
4. **连接池上限（64）**：驱动层硬闸门，避免把 MySQL 的 `max_connections` 打满。

**为什么「不排队」很重要**

排队只是把压力从数据库搬到应用内存：请求在 goroutine / channel 里等着，连接池仍然会被慢查询占满，最终还是会压到 DB。非阻塞 + 快速失败才是真正的「减量」。

**还剩的缺口**

| 缺口 | 后果 | 补法 |
|---|---|---|
| 闸门是**每进程**的 | 10 个 Pod = 320 并发，可能超过 MySQL 容量 | 按 MySQL 实测容量定 `MYSQL_MAX_OPEN_CONNS`；或做全局令牌桶 |
| 没有熔断 | Redis/DB 持续慢时，每个请求仍要等超时 | 接 `sony/gobreaker`，失败率/延迟超阈值直接快速失败，半开探测 |
| 没有全局限流 | HTTP 层限流本身依赖 Redis，Redis 挂了会放行；Feed 读没挂限流 | 网关层按路由做本地/全局限流 |
| 无指标 | 不知道闸门拒绝多少、降级多少次 | 暴露 `db_gate_rejected_total`、`redis_fallback_total`、各级命中率 |
| Redis 无高可用 | 单点故障只能靠应用降级 | 哨兵/集群；本项目的假设是「Redis 会挂」，所以降级链路必须先成立 |

**闸门容量怎么跟着连接池走**
`db.NewDB` 在设置完 `SetMaxOpenConns` 之后调用 `dbguard.Configure(dbcfg.MaxOpenConns)`，容量取一半（至少 1）。这样 `MYSQL_MAX_OPEN_CONNS=128` 时闸门自动变成 64，不会出现「池子调大了、闸门还是旧值」的脱节。剩余一半留给写路径、事务、迁移以及其他包的查询。

**代码依据**
`middleware/redis/redis.go`、`config/loadconfig.go`（`RedisConfig.Normalize`）、`feed/service.go:28`、`video/video_cache.go:100-130`、`dbguard/dbguard.go`、`db/db.go`。

---

# 三、第三层：故障与反问（Q13-Q17）

## Q13. Redis 整个集群不可用，所有请求都降级到 MySQL，数据库如何避免被打垮？

**一句话结论**
靠「L1 + singleflight + 连接池上限 + 统一回源闸门 `dbguard`」把并发压住，所有 Feed 读路径的 DB 入口都已接入闸门；但这是**有上限的降级**，仍然没有熔断和全局限流。

**Redis 挂掉时发生什么**

```text
GetVideoByIDs:
  MGET 报错 / 超时
    → missedL2 = missedL1（全量降级）
    → 每个 miss 起 goroutine，走 singleflight
    → leader 抢 dbguard
        ├─ 抢到：查库
        └─ 抢不到：返回 ErrServiceOverloaded
    → 只要有 ID 失败，GetVideoByIDs 返回聚合错误
        → handler 映射 503（过载）/ 500（其他 DB 错误）

ListByFollowing:
    GET 连接错误（不是 IsMiss）
      → 直接落到 doListByFollowingFromDB
      → 抢 dbguard
          ├─ 抢到：昂贵 JOIN
          └─ 抢不到：ErrServiceOverloaded → 503
```

**四道防线及其真实作用**

| 防线 | 作用 | 局限 |
|---|---|---|
| L1 5s | 吸收同实例重复读 | 5s 后失效；跨实例无效 |
| singleflight | N → P（每 Pod 1 次） | 不是 N → 1；只压并发，不压总量 |
| `dbguard`（`MaxOpenConns/2`） | 限制同时回源的请求数 | 每进程；不排队 |
| 连接池 64 | 防止无限建连打满 MySQL | 超出的请求在应用侧排队，仍可能拖高延迟 |

闸门覆盖的入口：实体回源（Feed + `GetDetail`）、最新流冷区与回源、关注流 JOIN、话题标签、热榜 MySQL 兜底、话题流、`IsLiked` 批量查询。`IsLiked` 是例外策略：闸门满时它降级成 `is_liked=false`，不拖垮整页。

**「TTL 随机抖动」有用吗？**
**对 Redis 整体不可用完全无效。** 抖动防的是「大量 key 同时过期」，而这里是缓存层不响应了，key 过没过期没有意义。这是本题最常见的错误答案。

**还没做的（主动披露）**

1. **熔断**：DB 错误率 / 延迟超阈值时快速失败，给下游恢复窗口。没有真实流量校准阈值，所以没硬做。
2. **全局限流**：HTTP 层的限流是 Redis 计数，**Redis 挂了直接放行**，而且 Feed 读接口根本没挂限流。
3. **指标**：闸门拒绝数、降级次数、各级命中率都没有埋点。
4. **Redis 高可用**：没有哨兵 / 集群；架构上假设 Redis 会挂，靠应用层降级，而不是靠 Redis 不挂。
5. **「部分失败」的体验**：现在只要有 ID 回源失败就整页 5xx，客户端拿不到已成功的部分。这是「不静默短页」的代价；更精细的做法是返回 `degraded` 标记 + `missing_ids`。

**上生产会怎么补**

```text
网关层：按路由做全局 QPS / 并发限流，Feed 读优先保，写请求先削
应用层：
  1. Redis 熔断（gobreaker）：失败率高 → 直接跳过 L2
  2. DB 熔断：慢查询比例高 → 快速失败，半开探测
  3. 全局回源令牌桶（Redis 可用时用 Redis，不可用时退化为本地令牌桶）
  4. 闸门已统一，后续是压测校准容量
  5. 异步回写已有界，后续补丢弃计数
  6. 指标 + 告警：降级率、闸门拒绝、DB P99、连接池使用率
存储层：
  7. Feed 读走只读副本；主库只承担写
  8. 给查询设 statement timeout
```

**面试话术**
> 「Redis 全挂时我们是全量降级，singleflight 只能把 N 收敛到 P，所以真正的保护是两道闸门：连接池上限和应用层回源闸门。闸门是非阻塞的，拿不到配额就返回过载，因为排队只是把堆积从数据库搬到应用内存。所有 Feed 的 DB 入口现在都走同一个闸门，容量跟着连接池自动算。
> 但我不会说这就够了：闸门是每进程的，没有熔断，也没有全局限流。上线前我会先补指标，再用压测校准容量，最后才是熔断阈值 —— 没有数据就拍一个熔断阈值，比没有熔断更危险。」

---

## Q14. Redis 恢复后，旧请求的查询结果才写回缓存，可能覆盖较新的数据。你怎么解决这个竞态？

**一句话结论**
实体缓存已经用「失效版本号 + Lua CAS」解决了这个竞态：每次失效原子地 `INCR version` 并 `DEL entity`，读路径在查库前记下版本，回写时版本变了就丢弃。关注流的**列表成员**缓存仍不是严格的：在途的 `setCached` 可能把刚取关的一页写回去，影响限于该页 TTL（首页 30s / 深层 24h），但页内内容每次都会重新装配。

**竞态时间线（修复前 / 修复后）**

```text
t0    请求 A：L2 miss，读 version=7，开始查库
t1    A 从 MySQL 读到 likes_count=100
t1.5  点赞请求：MySQL 更新为 101
      → INCR version = 8；DEL video:entity:123（同一个 Lua 原子完成）
t2    A 的异步回写：CAS 比较 version，发现 8 != 7
      → 放弃写入，旧值 100 不会复活
```

如果没有版本号，t2 的 `SET` 会把 100 重新塞回 Redis 并活 1 小时 —— 这就是旧实现的问题。

**代码依据**

- 失效：`videocache/entity.go` 的 `invalidateScript`（`INCR` + `PEXPIRE` + `DEL` 一次 EVAL）
- 版本读取：`video/video_cache.go` 的 `EntityVersion`；Feed 用一次 MGET 批量取
- 回写：`video/video_cache.go` 的 `SetVideoEntityIfVersion` → `videocache.SetIfVersion`
- 异步调度：`video.ScheduleEntityWrite`（256 槽位，有界）

**为什么「先更新 DB，再删缓存」还是不够？**

这是标准的 Cache-Aside 顺序，它防的是「删缓存后、DB 更新前，有读请求把旧值填回来」：

```text
先删缓存 → 读请求 miss → 读 DB（旧值）→ 写缓存（旧值）→ DB 更新
                                              ↑ 旧值长期驻留
```

「先 DB 后删」避免了这个问题，但它防不住**已经在途的读**：

```text
读请求已经读完 DB（旧值），只是还没写回缓存
→ DB 更新 + DEL
→ 读请求的写回才发生
→ 旧值复活
```

本项目的异步回写把这个窗口进一步放大：写回不是在请求返回前完成，而是在一个独立 goroutine 里，可能晚几十毫秒甚至更久。

**四种修法，按严格程度排序**

### 方案 1：失效版本号 + Lua CAS（已实现）

关键点：版本号放在 **Redis 的失效 epoch** 上，不是给 `videos` 表加字段。

```text
失效（写路径）:
  EVAL "INCR ver; PEXPIRE ver 25h; DEL entity"   ← 原子，避免回写插在两条命令之间
读取（回源前）:
  MGET video:entity:ver:<id>...                  ← 批量，和实体 MGET 同一份预算
回写（查库后）:
  EVAL "if GET ver ~= 读到的版本 then return 0
        SET entity payload PX 1h; return 1"
```

为什么版本必须在**查库之前**读：失效可能发生在「查库之后、回写之前」，那时查回的是旧值，CAS 必须失败。读不到版本（Redis 故障）时本次不回写，因为比较基准不可信。

为什么版本 TTL 要 25h：它必须明显长于实体 TTL（1h）。版本比实体先过期的话，一次迟到的旧值回写会重新通过校验。

```lua
-- KEYS[1] = entity, KEYS[2] = version
local cur = redis.call('GET', KEYS[2])
if (cur or '') ~= ARGV[2] then
  return 0   -- 期间发生过失效，旧值丢弃
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
return 1
```

- 优点：从根上消除「旧写覆盖新写」，不依赖时序，也不需要延迟双删那种 sleep。
- 代价：每次 miss 多一次版本 MGET；回写从 `SET` 变成 `EVAL`。
- 边界：它保证「失效之后不会写回旧值」，但**不解决「两个写请求本身乱序更新 DB」** —— 那是写路径的 CAS/版本问题，缓存层管不了。

### 方案 2：延迟双删（便宜，但不是证明）

```text
DB 更新 → DEL 缓存 → 延迟 500ms~1s → 再 DEL 一次
```

第二次删除覆盖「在途读」的写回窗口。问题是：

- 延迟多久？取决于最慢的读请求，没有上界；
- 延迟期间仍然读到旧值；
- 多一次 `DEL`，热门视频上放大写压力。

它能把窗口从「1 小时」压到「延迟时间」，但不是严格正确。适合作为过渡方案。

### 方案 3：写穿透 + 版本（适合写少读多）

DB 更新后直接把新值 `SET` 进缓存（而不是 `DEL`），并带版本号。读请求的旧值回写被版本 CAS 拒绝。好处是读路径不再 miss；坏处是写路径要知道完整的实体内容，且并发写仍可能乱序（需要版本）。

### 方案 4：拆分高频字段（结构性修复）

把 `LikesCount` / `Popularity` 从 `video.Video` 的缓存里拆出去：

- 不可变内容（标题、URL、封面）缓存 1h；
- 点赞数用独立的 Redis 计数器（或短 TTL key），读时合并；
- 点赞只更新计数器，不再 `DEL` 整个实体。

这样从根上减少「读旧实体 + 写回」的机会，也解决 Q10 里「越热越容易失效」的反模式。代价是读路径多一次合并，结构更复杂。

**本项目选哪个？**
选方案 1，已落地（见上）。面试时应该：

1. 先画时间线，说明「先 DB 后 DEL」防不住已在途的读；
2. 说明修复的三个细节：失效要原子、版本要在查库前读、版本 TTL 要长于实体 TTL；
3. 主动说边界：CAS 保证「失效后不回填旧值」，不保证「并发写本身有序」；
4. 再说没做的：拆分高频字段（点赞数独立计数器）仍是结构优化项，因为它影响命中率，不影响正确性。

**剩下的竞态：关注流列表成员**
`ListByFollowing` 的 `setCached` 也是在 DB 查询之后写 ID 页；并发取关的 `DelByPattern` 可能被在途的 `setCached` 覆盖，导致刚取关的作者视频重新出现。影响被两个设计压小：

- 页内内容每次 hydrate，作者信息/点赞/删除状态不会冻结；
- 首页 TTL 只有 30s，只有深层页会持续到 24h。

要严格解决同样需要版本号（按 account 或按页），但收益低于实体路径，暂不做。

---

## Q15. 本地缓存没有及时失效，用户更新头像后，为什么不同实例可能返回不同结果？如何处理跨实例缓存失效？

**一句话结论**
因为 L1 是**每个进程各自的一份内存**，写路径只删 Redis，没有任何跨实例通知。不同 Pod 的 L1 在不同时刻过期，所以在窗口内会返回不同结果。处理方式只有三类：缩短 TTL、广播失效、或者让强一致字段绕开 L1。

**先纠正一个前提：本项目 Feed 里没有头像**
`FeedAuthor` 只有 `ID` 和 `Username`（`feed/service.go:1134`，`buildFeedVideos`），没有 `AvatarURL`。所以「更新头像导致 Feed 不一致」在本项目不成立。但同类问题真实存在，而且更严重：

| 字段 | 存在哪 | 更新时会失效什么 | 实际脏多久 |
|---|---|---|---|
| `videos.username` | `videos` 表的反范式列 + 实体缓存 | 改名事务同步所有历史视频的 `username`，并逐个失效实体缓存；关注流只存 ID，hydrate 时重算 | 其他实例的 L1 ≤ 5s；DB 不再是永久旧值 |
| `LikesCount` / `Popularity` | `videos` 表 + 实体缓存 | 只删实体（版本 +1）；关注流 hydrate 重算 | 其他实例的 L1 ≤ 5s |
| 视频标题 / 封面 / 删除 | `videos` 表 + 实体缓存 | 只删实体（删除还移出热时间线）；关注流 hydrate 重算 | 其他实例的 L1 ≤ 5s |

> `Rename`（`account/service.go`）现在在**同一个事务**里更新 `accounts.username`、重签 token，并 `UPDATE videos SET username=? WHERE author_id=?`，返回受影响的视频 ID；事务提交后逐个 `videocache.Invalidate`。不这么做的后果是：旧视频永久显示旧名字，且实体缓存还会再冻 1 小时。

**为什么不同实例结果不同**

```text
Pod A 的 L1：video:entity:123 = 旧值，还剩 3s
Pod B 的 L1：video:entity:123 = 新值（或已过期，回源拿到新值）
用户连续刷新，负载均衡把请求分到 A / B
→ 同一个视频，两次响应不一样
```

L1 的 TTL 是 5s，所以这个窗口最多 5s。**这是 L1 短 TTL 存在的原因，也是它无法消除的代价。**

**跨实例失效的四类方案**

| 方案 | 做法 | 优点 | 缺点 | 本项目是否适用 |
|---|---|---|---|---|
| **短 TTL（现状）** | L1 只活 5s | 零基础设施，简单 | 仍有 5s 不一致；无法做到「立即」 | 适用，Feed 能忍 |
| **Pub/Sub 广播** | 写路径 `PUBLISH cache:invalidate video:entity:123`，每个 Pod 订阅后删本地 key | 延迟低（毫秒级） | Pub/Sub 是 at-most-once，Pod 离线/重启会漏消息；仍需 TTL 兜底；增加一条链路 | 可以加，但不解决「严格立即」 |
| **版本号 / epoch** | 每个 key 带版本，L1 存版本；读时和共享版本比对 | 不依赖消息可靠投递 | 每次读都要查共享版本 → 等于多一次 Redis 读，L1 的意义被削弱 | 不适合高频读 |
| **强一致字段绕开 L1** | 资料页 / 「我的主页」直接读 Redis 或 DB，不走 L1 | 真正立即；实现简单 | 这些读没有 L1 加速 | **推荐**：按字段选路径，而不是给所有字段都上 L1 |

**更彻底的方案：L1 只缓存「不可变字段」**
如果实体缓存只存标题、URL、封面、创建时间这类不常变的字段，把用户名、头像、点赞数排除在外，那么「更新头像」根本不会命中脏缓存。这是 Q14 方案 4 的延伸。

**为什么本项目没做 Pub/Sub**
按收益排序：

1. Feed 对秒级旧数据不敏感，5s TTL 已经够；
2. Pub/Sub 不能保证消息送达，最后还是要 TTL 兜底，等于多了一套复杂度却没换来「严格一致」；
3. 真正需要立即生效的是「我的资料」，这类请求本来就不该走 Feed 的 L1。

所以选择的是「短 TTL + 强一致字段绕开」，而不是「广播失效」。

**如果面试官要求「必须立即」怎么办？**
回答：那就不能把该字段放进 L1。可以：

- 资料页走 Redis（不带 L1），写路径 `SET` 新值；
- 或者写路径用版本号，读路径校验版本；
- 或者直接读 DB（资料页 QPS 通常远低于 Feed 流）。

核心是：**L1 和强一致天生冲突，不要试图用广播把 L1 修成强一致。**

---

## Q16. 如果数据库查询也失败，你说的「接口零报错」是否仍然成立？简历里的「零报错」具体指什么？

**一句话结论**
**部分成立。** 「Redis 故障不导致 5xx」仍然成立；但 MySQL 查实体失败现在会**显式返回 500/503**，不再静默返回短页。「零报错」必须限定为「不因缓存层故障而报错」。

**把「零报错」拆成几种情况**

| 故障 | 实际行为 | 是否 5xx | 数据是否完整 |
|---|---|---|---|
| Redis 超时 / 报错，MySQL 正常 | `missedL2 = missedL1`，全部回源；本次不回写 | 否 | 完整（闸门未满时） |
| Redis 正常，MySQL 查实体部分失败 | 返回聚合错误，整页失败 | **是（500）** | 不返回残缺页 |
| 回源闸门满（任意入口） | `dbguard.ErrOverloaded` | **是（503）** | 不返回残缺页 |
| 关注流 MySQL 失败 | 返回 error | 500 | 不完整 |
| 最新流冷区 MySQL 失败 | 返回 error | 500/503 | 不完整 |
| 热榜 MySQL 兜底失败 | 返回 error | 500/503 | 不完整 |
| `IsLiked` 批量查询失败 / 闸门满 | 降级为 `is_liked=false` 并打日志 | 否 | **辅助字段不完整** |

**关键代码：`GetVideoByIDs` 现在会返回 L3 的错误**

```go
// 每个 ID 的失败都会被累计；gate shed 单独计数
if err := dbguard.Do(...); err != nil {
    return nil, err
}
...
if firstErr != nil {
    return result, fmt.Errorf("feed: hydrate %d/%d videos from DB failed (gate shed %d): %w",
        failed, len(missedL2), shed, firstErr)
}
return result, nil
```

为什么必须抛：返回「短页 + nil error」时，客户端会把数据库故障当成「到底了」，不会重试；热榜还会按已取出的 member 数推进 offset，把失败的那几条**永久跳过**。用 `%w` 包住原错误，handler 里的 `apierror.ClassifyHTTPStatus` 仍能识别 `ErrServiceOverloaded` 并映射 503。

代价要说清楚：现在**一条失败就整页失败**，已成功装配的部分不会返回。这是「宁可报错，也不静默缺数据」的取舍；更精细的 `degraded` 标记还没做。

**唯一仍然静默降级的地方：`IsLiked`**
`buildFeedVideos` 里 `BatchGetLiked` 也走闸门；失败时降级成「都没点过」并记日志。它不改变视频内容是否完整，只影响按钮状态；而且点赞写入有唯一索引兜底，重复点不会写坏数据。面试时要主动区分：**内容字段失败要报错，辅助字段失败可以降级**。

**简历里的「零报错」到底该怎么写**

- 现在这句话的准确含义：**Redis 故障期间，读接口不会因为 Redis 报错而返回 5xx**。
- 不包含：MySQL 故障、闸门拒绝。这些现在会明确返回 500/503。
- 更严谨的写法：
  > 「Redis 不可用时读接口降级直查 MySQL，不因缓存故障返回 5xx；MySQL 也失败时快速失败并返回 5xx，不返回静默缺页的结果。」

**怎么改才算真正的「可控降级」**

1. **区分错误类型**：`GetVideoByIDs` 返回聚合错误并保留原错误链；已做。
2. **全失败/部分失败要报错**：已做（整页失败）。
3. **响应里标记部分失败**：返回 `degraded: true` / `missing_ids`，让前端提示「加载不完整」；未做。
4. **指标**：`entity_load_failed_total`、`partial_page_total`、`db_gate_rejected_total`；未做。
5. **区分「不存在」和「查询失败」**：已做。前者写哨兵，后者绝不写 —— 否则会把数据库故障缓存成「视频不存在」，持续 60 秒。

> 第 5 点尤其重要：如果查库失败时误写 `__null__`，故障会被放大成「大量视频消失」。当前代码只在 `len(videoList)==0` 且 `err == nil` 时写哨兵，这个边界是对的，要能说出来。

**「热点数据请求不再穿透 MySQL」也要限定**
它成立的前提是「L1 或 L2 命中」。缓存过期 / 冷启动 / Redis 故障时，请求仍然会到 MySQL —— 只是被 singleflight 合并成「每 Pod 每 key 一次」。不要把它说成「永远不会到 MySQL」。

---

## Q17. 如果热点 key 在 Redis 中过期，但本地缓存还没过期，是否会触发 MySQL 回源？什么情况下三级缓存仍然无法阻止击穿？

**一句话结论**
**不会。** L1 命中会在 `GetVideoByIDs` 的第一步直接返回，根本不会走到 L2，更不会到 MySQL。但三级缓存只能防「稳态热点读」，防不住「缓存整体缺失、跨 Pod 并发、DB 容量不足」这三类情况。

**先看代码顺序**

```text
L1 命中 → videoMap[id] = &data; continue
        → 如果所有 ID 都命中，直接 return buildOrderedResult(...)
```

`feed/service.go:134-157`。L1 在 L2 之前，所以「Redis 没有、L1 有」的结果就是：这次请求完全不碰 Redis 和 MySQL。

**但要先纠正一个常见前提**
本项目的 L1 TTL（5s）远小于 Redis TTL（1h）。正常情况下是 **L1 先过期，Redis 还在**，不是反过来。「Redis 过期但 L1 还在」只在以下情况出现：

- 写路径 `DEL` 了 Redis，但 L1 还没到期（例如刚点赞完）；
- Redis 被 flush / 重启 / AOF 丢数据；
- Redis 内存满，写入失败但旧 key 已不在；
- 人工删了 key。

这些情况下，L1 会挡住最多 5 秒；5 秒后仍然要回源。

**三级缓存防不住击穿的 8 种情况**

| # | 情况 | 为什么防不住 | 本项目现状 |
|---|---|---|---|
| 1 | **跨 Pod 并发** | singleflight 是进程内的，P 个 Pod 就是 P 次回源 | 存在；N → P |
| 2 | **所有 Pod 的 L1 同时失效 + Redis 没有 key** | 发布重启 / Redis 重启 / flush，所有实例同时冷 | 存在；只能靠闸门限流 |
| 3 | **多个热点 key 同时失效** | 每个 key 各 1 次/Pod，P × K 次回源 | 存在；闸门按请求数限，不按 key |
| 4 | **热点 key 在 Redis 被删/丢失** | 写路径每次点赞都 `DEL` 实体，热门视频反复 miss | 存在；L1 5s + singleflight 扛 |
| 5 | **leader 卡住** | `Do` 不接收 ctx，follower 永久阻塞；DB 慢查询会把 goroutine 堆满 | 存在；B2 短板 |
| 6 | **闸门已满** | 回源请求被主动拒绝，整页返回 503 | 存在；非阻塞降级，不再静默缺条 |
| 7 | **连接池耗尽** | 回源请求在池上排队，P99 飙升；更多请求堆积 | 存在；64 上限 |
| 8 | **热榜快照缺失后回落 MySQL** | `ZRevRange` 对不存在的 key 返回空且不报错，首页会落到 `ListByPopularity`；该路径已接闸门，但仍然是「缓存缺失 → 打 DB」 | 存在；闸门兜底 |

**第 8 点展开（很容易被忽略）**

`ListByPopularity` 的逻辑：

```go
f.ensureHotSnapshot(asOf)
members, err := f.rediscache.ZRevRange(readCtx, dest, start, stop)
if err == nil && len(members) == 0 {
    if offset > 0 { return 空页 }
    // offset == 0 时继续往下走
}
...
if err := dbguard.Do(func() error {          // ← 已接入统一闸门
    videos, err = f.repo.ListByPopularity(...)
    return err
}); err != nil {
    return resp, err                          // ← 过载映射 503
}
```

- 快照重建失败时，`ensureHotSnapshot` 只打日志；
- `ZRevRange` 对不存在的 key 返回空、不报错；
- 首页请求于是落到 MySQL 的 `ORDER BY popularity DESC`；
- 这条查询已经接闸门，Redis 故障时不会无限打 DB，但「快照缺失 → 回源」的放大效应本身仍在。

这是一个「缓存层故障 → 静默降级 → 打 DB」的典型链路，也是「三级缓存防不住」的实例。

**三级缓存到底防住了什么**

| 防住了 | 没防住 |
|---|---|
| 稳态下热点视频的重复读 | 缓存整体缺失时的冷启动 |
| 同一 Pod 内同 key 的并发回源 | 跨 Pod 的并发回源 |
| 已存在数据的读放大 | 不存在数据的枚举（哨兵只兜 60s） |
| 短时间内的 Redis 抖动 | DB 本身的容量上限 / 慢查询 |
| 单条视频的击穿 | 大量 key 同时 miss 的雪崩 |

**如果要进一步加强**

1. **把 singleflight 提前到 L2 之前**：按「整页 ID 集合」合并，能减少 1000 次 MGET，但不同页难合并，收益有限。
2. **跨 Pod 合并**：用 Redis SETNX 保护实体回源。按 6.2 的账，主键查询用 SETNX 不划算（省 0.9ms DB，付 600ms Redis），所以不做。
3. **逻辑过期**：value 里带逻辑过期时间，过期后仍返回旧值 + 异步重建。可以彻底消除「过期瞬间回源」，但会引入「读到过期数据」的语义，且需要额外的重建锁。
4. **热点探测 + 主动续期**：识别 QPS 最高的 key，在 TTL 到期前主动延长（比如 1h → 2h），避免热点 key 过期。这是生产上常见的做法，本项目没做。
5. **统一闸门已做；剩下熔断 + 指标**：见 Q13。

**面试话术**
> 「L1 命中就不会回源，这是代码顺序决定的。但三级缓存不是万能的：singleflight 只到 Pod 级，L1 和 Redis 可能同时缺失，热门视频的实体缓存还会被每次点赞删掉。真正防不住的是『缓存整体缺失 + 跨 Pod 并发 + DB 容量不足』这三件事同时发生。所以我补了连接池上限和回源闸门，但它们也只是把伤害限制在可控范围，不是消除。」

---

# 四、17 题一页速答

| # | 问题 | 一句话答案 |
|---|---|---|
| 1 | 为什么三级？只用 Redis？L1 的问题？ | 内存→网络→磁盘的代价阶梯；只用 Redis 热点集中、每次都有 RTT；L1 带来多实例不一致、内存不可控、冷启动、排障难 |
| 2 | TTL 怎么定？ | L1 5s = 跨实例脏窗口；实体 1h = DEL 失败兜底；关注流首页 30s / 深层 24h = 成员变化上界；都是工程估算，不是压测调参 |
| 3 | 穿透/击穿/雪崩？ | 击穿=singleflight/SETNX；穿透=`__null__` 哨兵 60s；雪崩=连接池 64 + 统一闸门 32；回写用版本 CAS 防旧值复活；抖动只防「同时过期」，不防「Redis 挂了」 |
| 4 | singleflight vs mutex？ | 按 key 合并并发并共享返回值；mutex 只串行化，等待者还要自己执行；用 mutex 要自己管 per-key 锁 + double check + 结果传递，等于重造 singleflight |
| 5 | MGET 优势？ | 1 次 RTT vs N 次；代价是全有或全无，所以降级也是全量；集群下注意 CROSSSLOT |
| 6 | 1000 并发同一视频，DB 几次？ | 单 Pod 1 次；P 个 Pod P 次；Redis 仍收 1000 次 MGET；leader 的 ctx 取消会连累所有 follower |
| 7 | Group 粒度？ | 每类数据源一个 Group，key 带实体 ID；全局重建用全局 key，分页用参数化 key；判据是「结果能否互换」 |
| 8 | 合并了但没写缓存？ | 当前批正常；同 Pod 5s 内靠 L1；之后重新回源；singleflight 不是缓存，压并发不压总量 |
| 9 | 本地缓存怎么实现？ | go-cache：RWMutex + map + 过期时间 + janitor；无容量上限、无 LRU；过期懒判断 + 5s 清理；可换 ristretto/bigcache |
| 10 | 实体 vs 列表？点赞删什么？ | 实体共享、列表因人而异；关注流列表只存 ID+游标，点赞只删实体，页内字段 hydrate 时重算，不再冻 24h；剩余问题是实体里混了 LikesCount 高频字段 |
| 11 | MGET 100 缺 20，怎么批量？ | 现状 20 个 goroutine 单查，为了按 ID 合并；可改「先批量查再按 ID 分发」，代价是合并率下降；批量与合并是矛盾的 |
| 12 | Redis 客户端怎么配？降级怎么不放大？ | 已开 `ContextTimeoutEnabled=true`，50ms 真实生效；池 64/空闲 8/池等待 200ms/读写 1s/重试 1；降级靠 L1+singleflight+统一闸门+不排队；回写有界 256 |
| 13 | Redis 全挂，DB 怎么不被打垮？ | 连接池 64 + 闸门 `MaxOpenConns/2` + L1 + singleflight；所有 Feed DB 入口已接入；没有熔断、没有全局限流、闸门仍是每进程；抖动无效 |
| 14 | 旧请求回写覆盖新值？ | 已用「失效版本号 + Lua CAS」：失效原子 INCR+DEL，回写版本不符就丢；版本读不到不回写；关注流列表成员竞态仍在（影响限于页 TTL） |
| 15 | L1 未失效，跨实例不一致？ | 每个进程一份，没有广播；头像不在 Feed，但 username 是反范式列，改名已在事务内同步 videos 并失效实体；跨实例仍最多脏 5s；方案：短 TTL / Pub/Sub / 强一致字段绕开 L1 |
| 16 | DB 也失败，还零报错？ | 只对「Redis 故障」成立；MySQL 失败现在整页 500/503，不再 200 短页；唯一静默降级的是 `IsLiked` 辅助字段；简历要限定范围 |
| 17 | Redis 过期但 L1 未过期，会回源吗？ | 不会，L1 先命中；但 L1 5s 后仍会回源；防不住跨 Pod、整体冷启动、多 key 同时 miss、热榜快照缺失回落 MySQL（已接闸门） |

---

# 五、面试官最可能追的 4 题：追问链

## 5.1 Q6 追问链

```text
1000 并发同一视频，DB 几次？
  → 单 Pod 1 次，为什么？singleflight
  → 多 Pod 呢？P 次，为什么？Group 在进程内存
  → 为什么不用 Redis 锁做成 1 次？算账：省 (P-1)×0.1ms，付 N×2 次往返
  → leader 挂了/客户端断开呢？follower 一起失败（ctx 绑定 leader）
  → follower 能取消吗？Do 不接收 ctx，要用 DoChan
  → leader panic 呢？等待者收到 panic / errGoexit
  → 结论：singleflight 是「并发合并器」，不是「分布式锁」，也不是「缓存」
```

## 5.2 Q12 追问链

```text
50ms 超时怎么做的？
  → context.WithTimeout(50ms) + MGET
  → 真的 50ms 吗？现在是真的：开了 ContextTimeoutEnabled
  → 以前呢？默认 false，ctx 不传给 socket，实际受 3s ReadTimeout
  → 连接池多大？64 / 空闲 8，池等待 200ms，重试 1
  → 池满了呢？200ms 拿不到连接就报错，回源兜底
  → 降级到 DB 怎么防雪崩？L1 + singleflight + dbguard + 池 64
  → 闸门是全局的吗？不是，每进程；容量 = MaxOpenConns/2，随连接池伸缩
  → 覆盖所有 DB 入口吗？Feed 的入口都接了，GetDetail 也接了
  → 那还缺什么？熔断 + 全局限流 + 指标
```

## 5.3 Q14 追问链

```text
先删缓存还是先更新 DB？
  → 先 DB 后 DEL（Cache-Aside）
  → 为什么不是先删？删后读 miss 会把旧值填回来
  → 先 DB 后 DEL 就安全吗？不，已经在途的读会在 DEL 之后回写旧值
  → 你们怎么防？失效时 INCR version + DEL 原子执行；回写版本 CAS
  → 版本什么时候读？查库之前；查库后才失效的话 CAS 必须失败
  → 版本读不到呢？本次不回写，比较基准不可信
  → 版本 TTL 多久？25h，必须长于实体 1h
  → 关注流呢？列表只存 ID，内容不脏；列表成员竞态仍在，影响限于页 TTL
  → 还做吗？拆高频字段（点赞数独立计数器）是命中率优化，不是正确性
```

## 5.4 Q16 追问链

```text
你说接口零报错，DB 挂了呢？
  → 实体路径返回聚合错误，整页 500；闸门满则 503
  → 为什么不返回部分数据？短页会被客户端当成「到底了」，热榜还会跳过失败项
  → 关注流呢？同样 500/503
  → 那「零报错」到底指什么？仅指 Redis 故障不导致 5xx
  → 还有静默降级吗？`IsLiked` 查不到时降级 false，因为它只是辅助字段
  → 怎么继续改？degraded 标记 / missing_ids / 指标
  → 查库失败会写哨兵吗？不会，只有查到 0 条才写，这个边界是对的
```

---

# 六、旧文档纠错：哪些话现在不能背

当前代码（2026-10-27）已经比 10 月 24 日的文档多了几处修改。下面这些旧表述需要更新，否则面试时会和代码对不上。

| 旧文档里的说法 | 位置 | 当前事实 |
|---|---|---|
| 「缓存穿透没有防护，是已知短板」 | `start-learn/缓存体系.md` §6、`learn/feed模块学习文档.md` §3.5 | **已修**：`__null__` 哨兵 60s，`GetVideoByIDs` 和 `GetDetail` 都接上了 |
| 「Redis 宕机没有熔断/限流兜底」 | `start-learn/缓存体系.md` §6、`learn/feed模块学习文档.md` §3.5 | **部分修**：连接池 64 + 统一 `dbguard`（容量随池伸缩，覆盖全部 Feed DB 入口）；仍然没有熔断和全局限流 |
| 「关注流的 context 超时预算被复用」 | `learn/feed模块学习文档.md` §3.5、`已知问题清单` F4 | **已修**：`getCached` / `setCached` / `Lock` / 读缓存各自新建 ctx |
| 「热榜 `opCtx` 被 4 步共用」 | `已知问题清单` A1 | **已修**：`ensureHotSnapshot` 用独立预算，分页读取用 `readCtx` |
| 「热榜快照重建无并发保护」 | `learn/feed模块学习文档.md` §5.5 | **已修**：`ensureHotSnapshot` 用 singleflight 按 `asOf` 去重 + 双重检查；另有后台预生成 `StartHotSnapshotBuilder` |
| 「50ms 硬超时」 | 多份文档 | **旧代码确实不成立**（默认 `ContextTimeoutEnabled=false`，实际 3s）；**现已修复**，开 true + 1s 读写兜底 |
| 「L1 TTL 3 秒」 | `start-learn/缓存体系.md` §1.3 | **实际 5 秒**，3 秒只是未使用的默认值 |
| 「关注流整页缓存脏 24h」 | 旧版本汇总 | **已修**：列表只存 ID+游标，内容每次 hydrate；只有列表成员在深层页可能残留 |
| 「旧请求回写覆盖新值，未解决」 | 旧版本 Q14 | **已修**：失效版本号 + Lua CAS；版本读不到不回写 |
| 「改名不更新 videos.username」 | 旧版本 Q15 | **已修**：改名事务内同步 `videos.username`，提交后逐条失效实体缓存 |
| 「过载响应码不一致（500 vs 503）」 | 旧版本 Q13/Q16 | **已修**：所有 Feed handler 统一走 `apierror.ClassifyHTTPStatus`，过载 503、其他 DB 错误 500 |
| 旧行号（`service.go:35-110` 等） | `learn/feed模块学习文档.md` §3 | 代码新增了哨兵、闸门、版本 CAS、列表缓存改造，行号整体后移，引用时以当前文件为准 |

**仍然成立的短板（可以继续讲）**

| 短板 | 位置 | 影响 |
|---|---|---|
| `singleflight.Do` 不接收 ctx | `feed/service.go` 实体回源 | leader 卡死时 follower 永久阻塞；客户端断开也无法退出 |
| 冷启动按 ID 单查 | `feed/service.go` `GetVideoByIDs` | 一页 20 个冷 ID → 20 条单行 SQL |
| 关注流等锁固定 5×20ms | `feed/service.go` `ListByFollowing` | 高负载下大量请求最终仍落到 DB |
| 异步回写失败只打日志 / 满了丢弃 | `video/video_cache.go` | 无感知，但不影响正确性，下一次 miss 会再补 |
| 关注流列表成员竞态 | `feed/service.go` `setCached` | 并发取关时，在途写回可能让该页残留旧 ID；内容仍会 hydrate 过滤 |
| L1 没有跨实例失效 | `feed/service.go` 本地缓存 | 其他实例最多脏 5s；改名/点赞在 5s 后一致 |
| 无熔断 / 无全局限流 / 无指标 | `dbguard` 之外 | 持续故障时每个请求仍要等超时；闸门拒绝量不可观测 |
| 部分失败导致整页 5xx | `GetVideoByIDs` | 不返回残缺页，但客户端也拿不到已成功的部分；可用 `degraded` 标记改进 |

---

# 七、不看答案自测（14 题）

> 能连续答出 10 题以上，这一章就算过了。

1. 三级缓存每一级是什么、TTL 多少、TTL 的含义分别是什么？
2. L1 实际 TTL 是 3 秒还是 5 秒？为什么代码里有两个数？
3. Redis 报错时是部分降级还是全量降级？为什么不能部分降级？
4. `MGET` 成功但返回 `nil` 是什么意思？哪些情况会被当成 miss？
5. `singleflight` 和 `sync.Mutex` 的本质区别是什么？为什么不用 mutex？
6. 1000 个并发查同一个过期视频，单 Pod / 多 Pod 各查几次 DB？Redis 收几次 MGET？
7. `singleflight.Group` 的 key 应该怎么设计？为什么 Feed 和详情页要共用一个 Group？
8. singleflight 合并了但缓存没写进去，下一批请求会怎样？为什么同 Pod 还能撑 5 秒？
9. L1 用什么实现？它怎么处理过期？有没有内存淘汰？可能出什么问题？
10. 点赞数变化时删了哪些缓存？关注流里为什么还会看到旧的 `IsLiked`？
11. 100 个 ID 里 20 个 miss，怎么组织 MySQL 查询？批量和 singleflight 为什么矛盾？
12. 你们的 Redis 客户端配了哪些超时？50ms 真的生效吗？降级时怎么防止 DB 被打垮？
13. Redis 全挂时，哪些 DB 入口有闸门、哪些没有？为什么说 TTL 抖动没用？
14. 旧请求回写旧值覆盖新值，时间线怎么画？版本 CAS 和延迟双删各解决到什么程度？

---

## 附：一页数字记忆卡

```text
L1：5s（默认 3s 未生效）/ janitor 5s
实体：1h          哨兵：60s        版本号：25h
关注流：首页 30s / 深层 24h（只存 ID）
单次 Redis ctx：50ms（已开 ContextTimeoutEnabled）
Redis 兜底读写：1s   池：64 / 8 / 等待 200ms / 重试 1
关注流锁：500ms   等锁：5 × 20ms
MySQL 池：64 / 16 / 1h
回源闸门：MaxOpenConns/2 = 32 / 进程（非阻塞）
异步回写：256 槽位（满了丢弃）
热榜：窗口 60 分钟 / 桶 TTL 2h / 快照 2min / Top 5000 / 合并 500ms
```
