# 冷热分离时间线 + Outbox 模块学习文档

> 覆盖链路：`发布视频` → `Outbox 本地消息表` → `RabbitMQ` → `Redis ZSET 热时间线` → `Feed 冷热分离查询`
>
> 本文配合代码阅读，文中所有「改前 / 改后」指的是本次对 4 个可靠性缺陷的修复。

---

## 0. 先回答一个问题：为什么需要 Outbox？

发布视频时要做两件事：

1. 往 MySQL 写一条 `videos` 记录；
2. 往 RabbitMQ 发一条「有新视频」的消息（消费端据此把视频 ID 写进 Redis 时间线 ZSET）。

**这两件事跨了两个系统，没法放进同一个事务。** 于是有四种写法：

| 写法 | 问题 |
|---|---|
| 先写库，再发消息 | 库写成功、消息发送失败 → 时间线里永远没有这个视频 |
| 先发消息，再写库 | 消息发了、库回滚 → 时间线指向一个不存在的视频 |
| 分布式事务（XA/TCC） | 能解决，但重得离谱，为一条时间线不值得 |
| **事务性发件箱（Outbox）** | ✅ 本项目采用 |

**Outbox 的核心思想**：把「要发的消息」当成一行数据，和业务数据写在**同一个数据库事务**里。事务一提交，消息就一定「存在」；再由一个后台 poller 把它读出来发出去。

```
BEGIN;
  INSERT INTO videos ...;          -- 业务数据
  INSERT INTO outbox_msgs ...;     -- 待发消息（同一个事务！）
COMMIT;
```

这样就把「跨系统原子性」问题，降维成了「单库事务 + 后台重投」问题。代价是**投递有延迟**（本项目约 1 秒），这就是简历里写的取舍。

代码位置：

```47:75:backend/internal/video/video_service.go
	//事务保证视频写入库和消息写入本地消息表的一致性
	err := vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(video).Error; err != nil {
			return err
		}

		msg := OutboxMsg{
			VideoID:    video.ID,
			EventType:  "video_published",
			Status:     OutboxStatusPending,
			CreateTime: video.CreateTime,
		}
```

---

## 1. 全链路数据流

```
┌─────────────── API 进程 ────────────────────────────────────────┐
│                                                                 │
│  POST /video/publish                                            │
│        │                                                        │
│        ▼                                                        │
│  VideoService.Publish ──[单库事务]──> videos 表                  │
│                                  └─> outbox_msgs 表(status=pending)
│                                                                 │
│  StartOutboxPoller (goroutine, 1s 轮询)                          │
│        │ ① ListDue   : 取到期消息                                │
│        │ ② Claim     : CAS 抢占 pending→processing               │
│        │ ③ PublishVideo : 发 MQ + 等 publisher confirm           │
│        │ ④ MarkSent / MarkRetry / MarkFailed                     │
│        ▼                                                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │ video.timeline.events (topic)
                            ▼
                 ┌──── RabbitMQ ────┐
                 │ queue:            │
                 │ video.timeline.   │
                 │ update.queue      │
                 └────────┬──────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│  StartConsumer (goroutine, 仍在 API 进程)                        │
│        │ 反序列化 TimelineEvent                                  │
│        │ ZADD feed:global_timeline  score=create_time(ms)        │
│        │                            member=video_id              │
│        │ ZREMRANGEBYRANK 0 -1001   ← 只保留最近 1000 条          │
│        ▼                                                        │
│  Redis ZSET: feed:global_timeline   ← 这就是「热时间线」         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│  FeedService.ListLatest(ctx, limit, latestBefore)               │
│    取 ZSET 最老一条的 score 作为 watermark                       │
│    reqTime > watermark  → 热区：ZREVRANGEBYSCORE 查 Redis        │
│    reqTime <= watermark → 冷区：create_time < ? 查 MySQL         │
│    热区不够 limit 条     → 用最后一条的时间缝合冷区（不回写 ZSET） │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 代码地图

| 文件 | 职责 |
|---|---|
| `backend/internal/video/video_entity.go` | `OutboxMsg` 结构体 + 状态常量 + 退避计算 |
| `backend/internal/video/outbox_repo.go` | Outbox 表的全部 SQL（查询/抢占/状态流转/回收/清理） |
| `backend/internal/video/video_service.go` | 发布视频，业务数据与 Outbox 同事务 |
| `backend/internal/worker/outboxworker.go` | `StartOutboxPoller`（投递）+ `StartConsumer`（写 ZSET） |
| `backend/internal/middleware/rabbitmq/timelineMQ.go` | 发布端封装，**publisher confirm** 就在这里 |
| `backend/internal/feed/service.go` | `ListLatest` 冷热分离查询（156-298 行） |
| `backend/internal/feed/repo.go` | 冷数据游标查询 |

---

## 3. Outbox 状态机

```
                    ┌──────────────────────────────────────┐
                    │                                      │
                    ▼                                      │
              [ pending ] ──Claim(CAS)──> [ processing ]   │
                    ▲                          │           │
                    │                          │           │
        MarkRetry   │                          ├─ 成功 ─> [ sent ] ──24h──> 删除
     (retry_count+1,│                          │
      指数退避)      │                          └─ 重试耗尽 ─> [ failed ]
                    │                                        (终态, 等人工)
                    │
        ReapStuck:  claimed_at 超时(60s) 未落地 → 回收回 pending
                （处理进程崩溃的场景）
```

### 为什么需要 `processing` 这个中间态？

如果只有 `pending / sent` 两态，多实例部署时两个 poller 会**同时查到同一批 pending 消息并各发一遍**。中间态 `processing` 就是一把「谁先改谁得」的锁。

### 为什么用 CAS 而不是 `SELECT ... FOR UPDATE SKIP LOCKED`？

```40:53:backend/internal/video/outbox_repo.go
// Claim 抢占一条消息：只有把 status 从 pending 改成 processing 的那个实例拿到发送权。
// 多实例部署时其余实例会拿到 RowsAffected=0，直接跳过，天然去重。
func (r *OutboxRepository) Claim(ctx context.Context, id uint, now time.Time) (bool, error) {
	tx := r.db.WithContext(ctx).Model(&OutboxMsg{}).
		Where("id = ? AND status = ?", id, OutboxStatusPending).
		Updates(map[string]interface{}{
			"status":     OutboxStatusProcessing,
			"claimed_at": now,
		})
	if tx.Error != nil {
		return false, tx.Error
	}
	return tx.RowsAffected == 1, nil
}
```

`SKIP LOCKED` 需要把「发消息到 RabbitMQ」这段网络 I/O 包在数据库事务里——事务会长达几十毫秒，锁竞争很痛。CAS 只在**一条 UPDATE** 上加行锁，拿到发送权之后再慢慢发。**安全性等价，性能更好**，这是面试可以说的一个点。

### 为什么需要 `ReapStuck`？

实例 A 抢占了消息（`processing`），还没发出去就崩了。这条消息会永远卡在 `processing`，没人再管它。所以需要有回收逻辑：`claimed_at` 超过 60s 还没落地，就放回 `pending`。

### 退避：`NextRetryAt` 解决忙轮询

```go
func OutboxBackoff(retryCount int) time.Duration {
	// 第 n 次重试等待 2s * 2^(n-1)：2s, 4s, 8s, 16s, 32s
}
```

失败的消息不是「下一轮立刻再试」，而是写入 `next_retry_at`，**到点才可见**：

```sql
WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at <= ?)
```

否则 RabbitMQ 一挂，poller 就会以每秒 100 条的速率疯狂重试，把 DB 和日志打爆。

---

## 4. 修复 ①：publisher confirm

### 改前（会丢消息）

```go
func (t *TimelineMQ) PublishVideo(...) error {
	return PublishJSON(ctx, t.ch, ...)   // 发出去就当成功
}
// 调用方：
if err == nil {
	db.Delete(&msg)   // ← 消息已删，但 broker 可能根本没落盘
}
```

`PublishWithContext` 返回 nil **只代表写进了 socket**，不代表 broker 收到了、更不代表持久化了。此时如果 broker 重启或连接断开，消息就丢了，而本地 outbox 行已经被删除，**永远补不回来**。

### 改后

```go
// 通道开启 confirm 模式
if err := ch.Confirm(false); err != nil { ... }
confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

// 发布后必须等到 broker 的 ACK 才算成功
if err := PublishJSON(ctx, t.ch, ...); err != nil {
	return err
}
return t.waitConfirm(ctx)
```

`waitConfirm` 有三种失败：NACK（broker 拒绝）、超时（5s）、通知通道关闭。任一失败都按「投递失败」处理，交给 Outbox 退避重试。

> ⚠️ 实现细节：这里「取下一条确认」而不是精确匹配 `DeliveryTag`，前提是 `PublishVideo` **串行调用**（poller 是单 goroutine 循环）。一旦超时，调用方会重建 channel，避免迟到的确认被下一条消息误当成自己的 ACK。这是刻意的简化，面试被问到要能讲出来。

---

## 5. 修复 ②：状态机取代「发布即删除」

改前是 `Delete(&msg)`，没有任何重试语义；改后是一条完整的流水线：

```go
claimed, err := repo.Claim(ctx, msg.ID, now)   // 抢占
if !claimed { continue }                       // 被别人抢走，跳过

err = tmq.PublishVideo(pubCtx, msg.VideoID, msg.CreateTime)  // 含 confirm

switch {
case err == nil:
	repo.MarkSent(ctx, msg.ID, time.Now())
case msg.RetryCount+1 > video.OutboxMaxRetries:
	repo.MarkFailed(ctx, msg.ID)              // 转终态，不再无限重试
default:
	repo.MarkRetry(ctx, msg.ID, msg.RetryCount+1, time.Now())
	_ = tmq.Close(); tmq = nil                 // 大概率是 channel 坏了，重建
	break                                       // 跳出本批，别撞同一条坏连接
}
```

注意最后两行：**投递失败后立刻重建 channel**。因为「一条失败」往往意味着整条连接都有问题，继续用同一条 channel 发下面 99 条只会全部失败。

---

## 6. 修复 ③：Poller 自愈（MQ 恢复后自动补发）

### 改前

```go
func StartOutboxPoller(db *gorm.DB, tmq *rabbitmq.TimelineMQ) {
	if db == nil || tmq == nil {
		log.Printf("Outbox poller disabled: ...")
		return        // ← 一次性判断，之后永远不再尝试
	}
```

启动时 MQ 不可用，`timelineMQ = nil`，poller 直接 return。**之后即使 MQ 恢复了，积压的消息也永远不会投递**，只能重启 API 进程。这与项目「RabbitMQ 可降级运行」的叙事直接矛盾。

### 改后

```go
if tmq == nil {
	t, err := rabbitmq.NewTimelineMQ(rmq)
	if err != nil {
		log.Printf("Outbox poller: TimelineMQ 未就绪(%v), %v 后重试", err, outboxRetryWait)
		sleepOrDone(ctx, outboxRetryWait)
		continue          // ← 回到循环顶部，下轮再试
	}
	tmq = t
}
```

`StartConsumer` 做了同样的处理。另外所有 `time.Sleep` 都换成了 `sleepOrDone(ctx, d)`，保证进程退出时 goroutine 不会卡在睡眠里。

**语义变化**：MQ 不可用期间，消息在 `outbox_msgs` 里安全积压；MQ 恢复后 poller 自动补发，Feed 最终补齐。这才是「降级不丢数据」。

---

## 7. 修复 ④：消费端不再无限 requeue

### 改前（死循环）

```go
if err != nil {
	log.Printf("写入Zset失败: %v", err)
	msg.Nack(false, true)   // requeue=true，无次数上限
	continue
}
```

Redis 挂了 → 消息写不进去 → Nack 重新入队 → 立刻又被取出来 → 又失败 → ……**无限循环**，还会把 Redis 恢复后的那一瞬间打爆。

### 改后（三级容错，不再丢弃）

现在时间线消费者和其他 Worker 一样复用统一的 `Consumer` 骨架（`worker/retry.go`），
队列也补齐了**重试队列 + 死信队列**：

```go
c := &Consumer{
	Name:  "TimelineConsumer",
	Queue: queueName,
	Retry: retry,                       // rabbitmq.RetryRouter
	Process: func(ctx context.Context, d amqp.Delivery) error {
		return processTimeline(ctx, redisClient, d)
	},
}
```

```
写 ZSET 失败
  → 进程内指数退避 3 次（1s/2s/4s + jitter）
  → 仍失败：投到 video.timeline.update.queue.retry，TTL 30s 后自动重投（最多 3 轮）
  → 仍失败：Nack(requeue=false) → video.timeline.update.queue.dlx 死信队列告警
```

**为什么改掉了「3 次后 Ack 丢弃」**：那是把 Outbox 辛辛苦苦保证的 at-least-once
在最后一环丢掉了，而且时间线少一条**没有任何报错**，是最难排查的一类故障。
现在最坏情况是消息躺在死信队列里等人处理，而不是消失。

> 代价：Redis 长时间不可用时，死信队列会持续堆积。这是刻意的——**宁可堆积可见的失败，
> 也不要静默丢弃**。详见下一节和 8.5 节。

---

## 8. 为什么「至少一次投递」不会造成脏数据？

因为 ZSET 的 member 是 `video_id`，重复 `ZADD` 同一个 member 只是**覆盖 score**，不会重复出现：

```251:262:backend/internal/worker/outboxworker.go
// zaddTimeline 单次写入。ZSET 的 member 是 video_id，重复写入是幂等覆盖，
// 所以"至少一次投递"不会造成脏数据——这正是 Outbox 可以放心重试的前提。
func zaddTimeline(ctx context.Context, redisClient *rediscache.Client, event *rabbitmq.TimelineEvent) error {
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	timelineKey := redisClient.Key("feed:global_timeline")
	if err := redisClient.ZAdd(opCtx, timelineKey, oredis.Z{
		Score:  float64(event.CreateTime),
		Member: fmt.Sprintf("%d", event.VideoID),
	}); err != nil {
		return err
	}
```

**这是整个设计的基石**：幂等性把「不重复投递」这个很难保证的性质，降级成了「重复了也无所谓」。所以我们可以放心地用 at-least-once + 重试，而不必追求 exactly-once。

同理，消费端丢一条消息的最坏后果是「某个视频暂时不在热时间线里」——它仍然在 MySQL 里，用户从冷区翻页照样能看到。这就是「时间线丢了可容忍」的含义。

---

## 8.5 两个高频追问（本次修复的重点）

### Q-A：broker 收到了消息，但 publisher confirm 丢了，怎么保证业务不重复执行？

**结论先给**：Outbox **保证不了「不重复投递」，只能保证「至少一次」**。去重责任**在消费端**，本项目靠的是 `ZADD` 的覆盖语义。但要做到这一点，必须同时满足两个条件。

#### 条件一：幂等键必须从写库那一刻起就固定（本次修掉的缺陷）

改前 `event_id` 是在发布时才生成的：

```go
// timelineMQ.go —— 改前
id, err := newEventID(16)      // 每次调用都生成一个新 ID
timeline := TimelineEvent{EventID: id, ...}
```

于是**同一条 outbox 消息重投时，带的 `event_id` 每次都不一样**。消费端就算想按它去重也完全去不掉 —— 幂等键形同虚设。

改后 `event_id` 在**写 outbox 的同一个事务里**生成并落库，poller 重投时从行里读出来原样带上：

```go
// video_service.go —— 写库时生成
eventID, err := rabbitmq.NewEventID()
msg := OutboxMsg{..., EventID: &eventID}

// outboxworker.go —— 投递时带上
eventID := ""
if msg.EventID != nil { eventID = *msg.EventID }
pubErr := tmq.PublishVideo(pubCtx, msg.VideoID, msg.CreateTime, eventID)
```

`OutboxMsg.EventID` 带唯一索引 `idx_outbox_event_id`，保证一条 outbox 消息 ⟺ 一个事件 ID。

#### 条件二：消费端的幂等要下沉到存储操作，而不是加一层有状态拦截

`ZADD(key, score=create_time, member=video_id)` 是**覆盖语义**，重复执行结果完全一致。

**这里刻意没有用 `event_id` 做阻断式去重**（比如 `SETNX` 占位），理由值得单独记住：

| 方案 | 问题 |
|---|---|
| 先 `SETNX` 占位，再写 ZSET | 占位成功但 ZSET 写失败 → 后续重试全被占位挡住 → 这条时间线**永久少一条**。去重反而制造了丢数据。 |
| 先写 ZSET，成功后再标记 | 标记失去了拦截作用，等于没做。 |
| **只靠 ZADD 覆盖语义** ✅ | 重复执行无害，且不引入任何新失败模式。 |

**一句话原则：幂等能下沉到存储操作就下沉，不要在消费端加一层有状态拦截。**

那 `event_id` 稳定化还有什么用？两个：
1. 日志里能追踪「同一条事件被投递了几次」；
2. 将来若给时间线加入**非幂等**副作用（计数累加、通知粉丝），它是唯一可用的去重键 —— 所以条件一必须先做，否则将来根本没法补。

---

### Q-B：Worker 抢占后崩溃，其他 Worker 怎么接管？

**机制**：`ReapStuck` —— `status='processing' AND claimed_at < now()-60s` 的消息放回 `pending`，下一轮 `ListDue` 被任何存活的 poller 重新捞走。

本次修掉了三个真实缺陷：

| # | 缺陷 | 后果 | 修复 |
|---|---|---|---|
| 1 | `MarkSent/MarkRetry/MarkFailed` 只校验 `status='processing'`，**不校验归属** | Worker A 卡住 61s 被回收 → B 抢占 → A 恢复后调 `MarkSent` **把 B 还没发成功的消息标成 sent** → 消息永久丢失 | **fencing token**：`Claim` 生成随机令牌写进 `claim_token`，所有状态迁移都带 `WHERE claim_token = ?`；旧持有者拿到 `ok=false` 时只记日志、不动状态 |
| 2 | `lastReap := time.Now()`，启动后要等 30s 才第一次回收 | 进程崩溃重启后，残留消息**白等 30 秒**才被接管 | 改成 `var lastReap time.Time`（零值），启动第一轮**立刻**回收 |
| 3 | `ReapStuck` 只改状态，不增加 `retry_count` | 「抢占→崩溃→回收→抢占」可**无限循环**，永远不进 `failed`、没有告警出口 | 回收**同样消耗一次重试额度**，超限转 `failed`；同时清 `claim_token`、把 `next_retry_at` 置空让它立即可见 |

#### fencing token 时序（这是最容易被追问漏掉的一环）

```
T0   A: Claim(id=7)      → processing, claim_token = tok1
T0   A: PublishVideo 卡住（MQ 半死不活）
T60  B: ReapStuck        → pending,    claim_token = NULL
T60  B: Claim(id=7)      → processing, claim_token = tok2
T61  A: 恢复，MarkSent(id=7, tok1)
        → WHERE claim_token='tok1' 不匹配 → ok=false
        → A 只记日志「已被其他实例接管」，不动状态 ✅
T61  B: 投递成功，MarkSent(id=7, tok2) → ok=true → sent
```

没有 token 时，A 的 `MarkSent` 会命中 `status='processing'` 把 B 的消息标成已发送 ——
**消息其实还没发成功，却永久性地不会再发了。这是这个模块最隐蔽的一个丢消息窗口。**

#### 无法消除的权衡

租约超时**无法区分「进程死了」和「进程只是慢」**。所以慢的情况下一条消息会被投递两次。
这由两层各司其职兜住：

- **重复投递** → 消费端幂等（上一节）；
- **数据安全** → fencing token（本节）。

宁可激进回收导致重复，也不能保守到让消息卡死 —— **卡死就是丢**。

---

## 9. 冷热分离查询

```156:167:backend/internal/feed/service.go
// 查询最新视频 (冷热分离 + 游标分页)
func (f *FeedService) ListLatest(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListLatestResponse, error) {
	if f.rediscache == nil {
		return f.listLatestFromDB(ctx, limit, latestBefore, viewerAccountID)
	}

	// 获取 ZSET 中最老的一条数据
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, f.rediscache.Key("feed:global_timeline"), 0, 0)
```

### 三步判断

**第一步：取水位线。** `ZRANGE key 0 0 WITHSCORES` 拿到 ZSET 里**最老**那条（因为 ZSET 按 score 升序，index 0 是最小的），它的 score 就是 `watermark`——热区的时间下界。

**第二步：比游标。**

```218:239:backend/internal/feed/service.go
	if reqTime <= watermark {
		//冷数据降级查库
		...
	} else {
		// 热数据直接查redis
		maxScore := "+inf"
		if !latestBefore.IsZero() {
			maxScore = fmt.Sprintf("%d", reqTime-1) // 防重复
		}
		videoIDsStr, err := f.rediscache.ZRevRangeByScore(ctx, ..., maxScore, "-inf", 0, int64(limit))
```

- 请求游标比水位线新 → 还在热区 → 查 Redis，一条 SQL 都不用
- 请求游标比水位线老 → 已经翻出热区 → 查 MySQL

`reqTime-1` 是**严格小于**，保证翻页不会重复拿到上页最后一条。

**第三步：缝合。** 热区凑不满 `limit` 条（刚好跨过冷热边界）时，用热区最后一条的 `CreateTime` 当冷游标，从 MySQL 补齐剩余：

```259:277:backend/internal/feed/service.go
		// 刚好击穿了冷热边界
		if len(baseVideos) < limit {
			remainLimit := limit - len(baseVideos) // 计算还差几个
			var coldCursor time.Time
			if len(baseVideos) > 0 {
				coldCursor = baseVideos[len(baseVideos)-1].CreateTime
			} else {
				coldCursor = latestBefore
			}
			...
			if err == nil {
				coldVideos := v.([]*video.Video)
				baseVideos = append(baseVideos, coldVideos...)
			}
```

**关键取舍**：补齐的冷数据**不回写 ZSET**（第 230 行注释），否则用户随便翻几页就能把历史数据灌进只保留 1000 条的热池，把真正的新视频挤出去——这就是「冷数据污染热点池」。

### 游标分页 vs OFFSET

```20:31:backend/internal/feed/repo.go
func (repo *FeedRepository) ListLatest(ctx context.Context, limit int, latestBefore time.Time) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("create_time DESC")
	if !latestBefore.IsZero() {
		query = query.Where("create_time < ?", latestBefore)
	}
	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}
```

- `OFFSET 100000` 要扫过前 10 万行再丢弃，越翻越慢；
- `WHERE create_time < ?` 走 `idx_videos_create_time` 索引，直接定位，**第 1 页和第 1000 页代价一样**；
- 且期间有新视频插入时，`OFFSET` 会导致「上页末尾重复出现 / 某些条目漏掉」，游标不会。

单靠 `create_time` 在同一毫秒多条时会不稳定，工业做法是 `(create_time, id)` 复合游标——热榜那边就是这么做的（`ListByPopularity` 用 `latestPopularity + latestIDBefore`）。时间线这里没做，属于已知简化。

---

## 10. 失败场景演练（面试常问）

| 场景 | 行为 | 结果 |
|---|---|---|
| 发布成功，MQ 恰好抖动 | confirm 超时/NACK → MarkRetry → 2s 后再试 | ✅ 最终投递成功 |
| RabbitMQ 整机宕机 5 分钟 | poller 每 5s 重建 TimelineMQ 失败，消息在表里积压；恢复后自动补发 | ✅ 不丢，延迟投递 |
| 进程在 `processing` 阶段崩溃 | 60s 后 `ReapStuck` 回收回 `pending`；进程重启后第一轮**立刻**回收 | ✅ 不丢 |
| 崩溃实例恢复后想改状态 | fencing token 校验失败（`claim_token` 已被刷新），只记日志 | ✅ 不会误标别人正在处理的消息 |
| 两个 API 实例同时轮询 | `Claim` 的 CAS 只有一个拿到 `RowsAffected=1` | ✅ 不重复投递 |
| 同一消息被重复投递两次 | ZSET member = video_id，覆盖写 | ✅ 幂等，无脏数据 |
| confirm 丢失但消息其实已落盘 | 按失败处理 → MarkRetry → 重投 | ✅ 消费端幂等，重复无害 |
| Redis 宕机期间消费消息 | 退避 3 次 → 重试队列重投 3 轮 → 死信队列告警 | ⚠️ 该视频暂不在热时间线，冷区仍可查到；消息在 DLQ 可查可重投 |
| 消息重试 5 次仍失败 | 转 `failed` 终态 + 打日志 | ⚠️ 需人工介入，不会无限重投 |

---

## 11. 面试追问清单

> 每条先给**一句话要点**（考场上先答这个），再给**完整回答**（被追问时展开）。


---

### Q1：为什么不用「直接双写」或者 MQ 事务消息？

**要点**：双写没有原子性；事务消息绑定 MQ 能力且要写回查逻辑。Outbox 把跨系统原子性收敛成单库事务，简单、可观测。

**完整回答**：
发布视频要写 MySQL + 发 MQ，这两个系统不在同一个事务里。直接双写有四种失败窗口：

| 顺序 | 失败点 | 后果 |
|---|---|---|
| 先写库再发消息 | 库成功、发消息失败 | 视频存在但时间线永远没有它 |
| 先发消息再写库 | 消息发出、库回滚 | 时间线指向一个不存在的视频 |
| 两边都成功但网络抖动 | 应用以为失败重试 | 重复消息 |
| 进程在中间崩溃 | 状态未知 | 无法判断该补哪边 |

MQ 事务消息（如 RocketMQ）能解，但要把「本地事务执行结果」回查接口写出来，且强绑定具体 MQ 产品。

Outbox 的思路是**降维**：既然跨系统没法原子，就把「要发的消息」当成普通数据行，和业务数据写进**同一个数据库事务**。事务一提交，消息就一定存在；剩下「把它发出去」这件事交给后台 poller，可以重试、可以观测、可以人工介入。

**加分点**：可观测性是 Outbox 被低估的优势——积压量就是 `SELECT COUNT(*) WHERE status='pending'`，一条 SQL 就能做告警，而 MQ 里的消息堆积往往不好查。

---

### Q2：Outbox 的代价是什么？

**要点**：① 投递延迟（本项目 ~1s）；② 轮询对 DB 有压力；③ 消息表会增长。三个都有对应缓解手段。

**完整回答**：
1. **投递延迟**：轮询间隔就是延迟下界。本项目 1s 轮询，所以发布后约 1 秒才进时间线。这是我用「发布写库的原子性」换来的，产品上（作者发布后自己刷新、粉丝刷 Feed）完全可接受。
2. **DB 压力**：每秒一次 `SELECT`。缓解手段是**批量取 100 条** + **失败退避**（`next_retry_at` 到点才可见）+ 状态字段有索引。没有退避的话，MQ 一挂就会变成每秒 100 条失败重试的空转风暴。
3. **表增长**：消息发成功不能删（否则没有审计和重试依据），所以改成 `sent` 状态 + 24 小时后 `PurgeSent` 清理。

**加分点**：延迟想更小可以降低轮询间隔到 100ms，但 DB 压力涨 10 倍，属于典型的吞吐/延迟取舍；真要毫秒级就上 CDC（见 Q14）。

---

### Q3：怎么保证不重复投递？

**要点**：严格说不保证。我们保证的是 **at-least-once**，重复由消费端幂等消化。`Claim` 的 CAS 只是把重复概率压到极低。

**完整回答**：
重复投递有两个来源，分别用两道防线处理：

- **多实例并发**：两个 poller 同时查到同一批 pending 消息。防线是 `Claim` 的 CAS——`UPDATE ... WHERE id=? AND status='pending'`，只有一个实例拿到 `RowsAffected=1`，其余拿到 0 直接跳过。
- **投递成功但状态没更新**（进程崩在中间）：消息发出去了，但 `MarkSent` 没执行，下轮再发一遍。这个 CAS 挡不住。

所以真正的兜底是**消费端幂等**：ZSET 的 member 是 `video_id`，重复 `ZADD` 同一个 member 只是覆盖 score，不会产生第二条记录。

**加分点**：exactly-once 在分布式系统里要么代价极高（分布式事务），要么根本不存在。工程上通用做法是「at-least-once + 幂等消费」，把难保证的性质换成容易保证的性质——这是这个模块最核心的设计思想。

---

### Q4：publisher confirm 和 AMQP 事务（tx_select）有什么区别？

**要点**：confirm 是「broker 落盘就 ACK」，轻量；tx 是真正的 AMQP 事务，性能差一个数量级。生产都用 confirm。

**完整回答**：
`PublishWithContext` 返回 nil **只代表数据写进了本地 socket**，不代表 broker 收到了，更不代表持久化了。如果此时 broker 重启或连接断开：

- 没有 confirm：本地以为成功 → 删掉/置 sent → **消息永久丢失**
- 有 confirm：broker 没回 ACK → 按失败处理 → Outbox 退避重试 → 最终投递成功

AMQP 事务（`tx_select / tx_commit`）是「多条消息要么都发成功要么都不发」，每批都要多轮网络往返，吞吐量大约掉一到两个数量级。而 confirm 是**单条粒度**、异步回执，可以流水线化，性能损失很小。

**加分点**：confirm 有两种用法——同步等（本项目，每发一条等一次，简单但吞吐低）和批量等（发 N 条后一起等，吞吐高但要处理失败重发）。时间线发布 QPS 很低，所以选了最简单的同步等。

---

### Q5：为什么时间线消费者留在 API 进程，不放到 Worker 进程？

**要点**：按消费特征分层——轻的、要求低延迟的留在 API；重的、需要独立扩容的拆到 Worker。

**完整回答**：
不要把「异步消费者都该独立部署」当成教条。我按三个维度给消费者分类：

| 维度 | API 内（时间线/通知） | Worker（点赞/评论/关注/热度） |
|---|---|---|
| 单次处理 | 一次 Redis 写 | MySQL 事务写 + 计数更新 |
| 失败代价 | 丢了最多时间线少一条，可容忍 | 丢了点赞数就错了，必须重试 + 死信 |
| 流量特征 | 平稳（跟随发布量） | 突发（点赞风暴会大量积压） |
| 延迟要求 | 高（刚发布的视频要马上能刷到） | 低（计数晚几百毫秒无所谓） |

关键在「突发」：点赞高峰期队列会堆起来，需要**加消费者副本去追**。如果消费者混在 API 进程里，为了多几个点赞消费者就得把整个 API 服务（连同全部 HTTP 路由、连接池、限流器）复制一份——又重又危险。拆出 worker 后 `docker-compose up --scale worker=5` 就解决了。

反过来，时间线消费量跟发布量走，很平稳，且要求低延迟，放进 API 进程省一次网络跳转。

**加分点**：代码里有个细节能佐证这个分层——Worker 侧 QoS 是 **50**（预取 50 条批量处理，追求吞吐），而时间线消费者 QoS 只有 **10**（少预取、少缓冲，追求低延迟、不抢 API 资源）。

---

### Q6：ZSET 只保留 1000 条够吗？

**要点**：这是明确的容量取舍，不是缺陷。1000 条之后走 MySQL + 索引游标，性能仍可接受。

**完整回答**：
`ZREMRANGEBYRANK key 0 -1001` 每次写入后裁掉最老的，只留最近 1000 条。选 1000 的考虑：

- **内存可控**：一条 member 是数字 ID，1000 条在几十 KB 量级，即使未来按标签/分频道扩展多个 ZSET 也不心疼。
- **热区的定义**：绝大多数用户刷 Feed 不会翻过前几屏，1000 条覆盖绝大部分真实请求。
- **超出后的代价可接受**：翻出热区就走 `WHERE create_time < ? ORDER BY create_time DESC LIMIT ?`，有 `idx_videos_create_time` 索引，代价是常数级的，不会劣化。

真正的代价是**热区覆盖率有限**——如果站点发布量很大（比如每秒几十条），1000 条可能只覆盖几分钟，热区命中率就很低了。这种情况下应该按发布速率动态调容量，或者干脆改成「按时间而非按条数」裁剪。

**加分点**：这个数字必须是**可配置的常量**而不是散落的魔法数字，要根据线上发布 QPS 定期回看。

---

### Q7：你最初的实现有什么问题？你是怎么发现并修的？

**要点**：四个缺陷——① 无 publisher confirm 会丢消息；② 无状态机多实例会重复投递；③ MQ 挂了 poller 永久退出不补发；④ 消费端无限 requeue 死循环。

**完整回答**（这是展示工程能力的最好机会，按「发现 → 定位 → 修复 → 残留」讲）：

1. **无 publisher confirm**：`PublishJSON` 发出就当成功，紧接着 `db.Delete(&msg)`。broker 未落盘时消息永久丢失且无法补回。**修复**：通道开启 confirm，收到 ACK 才置 `sent`。
2. **无状态机/抢占**：`SELECT * WHERE status='pending' LIMIT 100` 后直接发，多实例部署会各发一遍。**修复**：加 `processing` 中间态 + CAS 抢占 + `claimed_at` 崩溃回收。
3. **poller 一次性判空**：启动时 `tmq == nil` 就 `return`，MQ 恢复后积压消息永远不投递，必须重启 API。**修复**：改成循环内周期性重建，恢复即补发。
4. **消费端无限 requeue / 静默丢弃**：`Nack(false, true)` 无次数上限，Redis 故障期间消息被打成死循环；后来改成「重试 3 次后 Ack 丢弃」又走向另一个极端，把 at-least-once 在最后一环丢掉了。**修复**：复用统一 `Consumer` 骨架，改成三级容错（进程内退避 → 重试队列 TTL 重投 → 死信队列告警），全程不静默 Ack 失败消息。

**发现方式**：对着「降级/最终一致」的叙事逐条反推故障场景——MQ 挂、Redis 挂、多实例、进程崩溃——看代码在每种情况下到底会怎样。凡是「只在文档里成立、代码里不成立」的，都是漏洞。

---

### Q8：为什么用 CAS，不用 `SELECT ... FOR UPDATE SKIP LOCKED`？

**要点**：安全性等价，但 `SKIP LOCKED` 要把发 MQ 的网络 I/O 包进数据库事务，锁持有时间从一条 UPDATE 变成几十毫秒。

**完整回答**：
`SKIP LOCKED` 的经典写法是：

```sql
BEGIN;
SELECT * FROM outbox_msgs WHERE status='pending' LIMIT 100 FOR UPDATE SKIP LOCKED;
-- 发 MQ（网络 I/O，几十毫秒甚至更久）
UPDATE outbox_msgs SET status='sent' WHERE id IN (...);
COMMIT;
```

问题在中间那步：**事务要一直开着到 MQ 发完**。100 条消息 × 每条一次网络往返，事务可能持续几百毫秒，行锁锁住一批消息，其他实例的 poller 全部被挡住。MQ 抖动时事务时间会进一步拉长，甚至拖垮连接池。

CAS 的做法是把「抢」和「发」拆开：

```sql
UPDATE outbox_msgs SET status='processing', claimed_at=?
WHERE id=? AND status='pending';   -- 只有这一条语句加锁
```

拿到 `RowsAffected=1` 的就是归属者，之后慢慢发，全程不持锁。**多实例安全性完全等价**——都是「只有一个实例能把状态从 pending 改走」。

**加分点**：这体现了「锁粒度要覆盖临界区，但临界区要尽可能小」——CAS 把临界区从「发消息」缩小到「改状态」。

---

### Q9：`claimed_at` 超时 60s 回收，这个数字怎么定？会不会误回收？

**要点**：60s 要显著大于「抢占到落地」的正常耗时；真出现误回收也没事，因为消费端幂等。

**完整回答**：
正常路径是「Claim → 发一条消息 → MarkSent」，耗时在毫秒级。60s 留了两三个数量级的余量，覆盖 GC 停顿、DB 慢查询、MQ 抖动等极端情况。

定这个数字的原则是：**超时时间 > P99 处理耗时 × 一个足够大的系数**。如果业务里单次处理本来就很久（比如要调外部接口），就必须相应调大，或者引入「租约续期」——处理中定期更新 `claimed_at`，证明自己还活着。

**会不会误回收**？会，但后果可控：实例 A 处理超时（但没死），消息被回收成 `pending`，实例 B 又发了一遍 → 消息投了两次 → **消费端 ZSET 幂等覆盖**，最终结果完全一样。这就是幂等设计带来的容错空间。

**加分点**：注意区分「重复投递」（无害）和「消息丢失」（有害）。回收机制宁可激进一点导致重复，也不能保守到让消息卡死——因为卡死就是丢。

---

### Q10：confirm 超时之后，为什么要重建 channel？

**要点**：超时后迟到的 ACK 可能错位到下一条消息；重建 channel 可以彻底清掉这个歧义。

**完整回答**：
我的 `waitConfirm` 是「取下一条确认」，而不是精确匹配 `DeliveryTag`（前提是 `PublishVideo` 串行调用）。如果第 N 条超时了，它的 ACK 可能在第 N+1 条发布之后才到——那时我会把这个迟到的 ACK **误当成第 N+1 条的确认**，于是第 N+1 条明明没落盘却被判成功，就又回到「以为成功实则丢失」的老问题。

所以策略是：**一旦超时或 NACK，就把 channel 置为不可用并重建**。重建后迟到的确认随旧 channel 一起作废，不会污染后续判断。同时这条消息已经 `MarkRetry`，会在退避后重新投递，不会丢。

**加分点**：如果想做得更严谨，可以在发布前记录 `ch.NotifyPublish` 里的 `DeliveryTag` 做精确匹配（amqp091-go 的 `PublishWithDeferredConfirm` 就是干这个的）。本项目发布是串行、QPS 很低，用简化版足够，但我清楚它的前提和失效边界——**知道自己的简化在哪、什么时候会失效**，比盲目套复杂方案重要。

---

### Q11：游标分页和 OFFSET 到底差在哪？

**要点**：OFFSET 是「扫过并丢弃前 N 行」，越翻越慢；游标是「从索引位置直接开始」，每页代价相同。且 OFFSET 在并发写入下会重复/漏项。

**完整回答**：
`LIMIT 10 OFFSET 100000` 的执行过程是：先取出 100010 行，丢弃前 100000 行，返回 10 行。翻到第 10000 页时数据库要做十万行量级的无用功。

```sql
-- 游标写法：直接定位，代价与页码无关
SELECT * FROM videos WHERE create_time < ? ORDER BY create_time DESC LIMIT 10;
```

配合 `idx_videos_create_time` 索引，第 1 页和第 10000 页的代价几乎一样。

**更隐蔽的问题是翻页错乱**：用户看第 1 页时，有人发布了 3 条新视频。翻第 2 页时：

- OFFSET：整体后移 3 位，第 2 页开头会**重复**第 1 页末尾的 3 条
- 游标：`create_time < 上一页最后一条`，新数据插在游标之前，**不参与后续分页**，不重复也不漏

本项目热区用 `ZREVRANGEBYSCORE` + `reqTime-1`（严格小于），冷区用 `create_time < ?`，两端都是游标语义。

**加分点**：单用 `create_time` 在同一毫秒有多条时会不稳定（可能漏掉同毫秒的另一条）。工业做法是**复合游标** `(create_time, id)`，本项目热榜就是这么做的（`latestPopularity + latestIDBefore`）。时间线这边还只用 `create_time`，是已知的简化项。

---

### Q12：冷热边界怎么缝合？为什么冷数据不回填缓存？

**要点**：热区取不满 limit 时，用热区最后一条的 `CreateTime` 当冷游标去 MySQL 补齐；补齐结果**不写回 ZSET**。

**完整回答**：
跨边界的那一页会走三步：

1. 从 Redis 拿热区 ID 列表（`ZREVRANGEBYSCORE`），最多 `limit` 条；
2. 如果不足 `limit` 条，说明这一页跨过了冷热边界，用**热区最后一条的 `CreateTime`** 作为冷游标去 MySQL 补 `limit - len` 条；
3. 拼接返回。

关键点在**不回填**：如果不小心把补齐的冷数据写回 ZSET，那么任何用户多翻几页，就能把大量历史数据灌进只保留 1000 条的热池，**把真正的新视频挤出去**——这就是「冷数据污染热点池」。一次误操作就能让热区的命中率归零。

**加分点**：判断走热区还是冷区的依据，是**请求游标与 ZSET 最老一条 score 的比较**：

```go
if reqTime <= watermark { /* 冷区：查 MySQL */ } else { /* 热区：查 Redis */ }
```

这个 `watermark` 是动态计算出来的，不需要维护额外的元数据——ZSET 本身的状态就是冷热边界。

---

### Q13：Redis 全挂了 Feed 还能用吗？ZSET 空了怎么重建？

**要点**：能。`rediscache == nil` 或 ZSET 查询报错都直接降级 `listLatestFromDB`；ZSET 为空时用 singleflight 从 MySQL 拉最新 1000 条重建。

**完整回答**：
三层降级：

1. **Redis 整体不可用**：`ListLatest` 开头判断 `f.rediscache == nil`，直接走 `listLatestFromDB`，纯 MySQL 游标查询，服务可用（慢一些）。
2. **Redis 超时/报错**：取 `zsetTail` 出错也降级走 DB，不会把错误抛给用户。
3. **ZSET 还是空的**（冷启动 / Redis 被清）：触发重建——用 singleflight 保证只有一个请求去重建，从 MySQL 拉最新 1000 条 `ZADD` 回 ZSET，其余请求等待；如果 DB 也是空的，返回 `EMPTY_DB` 哨兵**防无限递归**。

**加分点**：空 ZSET 重建最怕的是「DB 也没数据 → 重建失败 → 又判定为空 → 再重建」的递归死循环，代码里用 `EMPTY_DB` 哨兵值显式切断了这个环。写这类「自愈」逻辑时，**先想清楚自愈条件永远不满足时会怎样**。

---

### Q14：1 秒的投递延迟能不能更小？为什么不上 CDC（Canal / Debezium）？

**要点**：能，轮询间隔调小即可，但 DB 压力线性上涨；CDC 能做到毫秒级且无轮询，代价是引入一套新组件和运维复杂度。

**完整回答**：
- **调小轮询间隔**：100ms 轮询 → 延迟降到 ~100ms，但 DB 查询频率涨 10 倍。时间线发布 QPS 本来就低，这个交换很不划算。
- **CDC（监听 binlog）**：Canal/Debezium 订阅 `outbox_msgs` 表的 binlog，数据一提交就推给 MQ，延迟可到毫秒级，且**没有轮询开销**。这是 Outbox 的工业级演进方向。
- **为什么本项目没上**：CDC 要引入一套独立的采集组件 + 位点管理 + 高可用部署，对一个单机演示项目来说运维成本远超收益。而且 binlog 解析出错时的定位难度远高于「一条 SQL 查 pending」。

**加分点**：能说出「当前方案在什么量级下会不够用，演进方向是什么」，比单纯说「我用了 Outbox」高一个层次。

---

### Q15：Outbox 表涨到千万行怎么办？

**要点**：分三层——先靠清理压住存量，再靠索引/分区保证查询不劣化，最后才考虑归档分表。

**完整回答**：
1. **控制存量**：已经发送的消息不需要永久保留。`PurgeSent` 每 10 分钟清理 24 小时前的 `sent` 记录，稳态下表里只有「未投递 + 近一天已投递」，行数与**投递速率**相关而不是与**历史总量**相关。
2. **保证查询不劣化**：热查询是 `WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at <= ?) ORDER BY id LIMIT 100`，需要 `(status, next_retry_at, id)` 的复合索引；`CountPending` 走 `status` 索引。只要行数在索引可控范围内，查询代价与总量无关。
3. **归档**：`failed` 是终态，长期堆积说明有系统性问题，应该告警 + 人工处理后归档到历史表。
4. **分区**：真到千万级，按 `create_time` 做范围分区，清理直接 `DROP PARTITION`，比 `DELETE` 快几个数量级。

**加分点**：不要一上来就说分库分表。Outbox 的特点是**稳态行数很低**（消息发完就清），真正会撑爆它的只有「投递长时间失败」这种异常场景——那本身就应该告警，而不是靠扩容掩盖。

---

### Q16：消息顺序能保证吗？

**要点**：不保证全局顺序，也不需要。时间线的顺序由 **ZSET 的 score（create_time）** 决定，与投递顺序无关。

**完整回答**：
消息可能乱序到达：重试机制会让失败的消息延后投递，`next_retry_at` 更是在显式制造乱序。

但这不影响正确性，因为**顺序是在 Redis 侧重建的**——ZSET 按 score 排序，score 取的是事件的 `create_time`，无论消息什么时候到、到了几次，最终排出来的顺序都由创建时间决定。

这是把「投递顺序」这个难保证的性质，转换成「按业务字段排序」这个容易保证的性质，和前面的幂等设计是同一类思路。

**加分点**：需要严格顺序的场景（比如「先创建后删除」不能颠倒）不能靠 MQ 顺序，要靠**版本号 / 状态机 / 幂等键**。本项目没有这类场景。

---

### Q17：消费端重试到上限后进了死信队列，数据不就丢了吗？怎么补偿？

**要点**：不会丢，消息在死信队列里可查可重投。而且即使真丢了也没关系——视频数据本身在 MySQL 里，丢的只是「它在热时间线里的位置」；ZSET 冷启动还会自动重建。

**完整回答**：
先分清两层数据：

- **源数据（videos 表）**：从来没丢过，Outbox 保证的是这条；
- **派生数据（ZSET 热时间线）**：丢了最多是这个视频暂时不出现在热区，用户翻到冷区照样能看到。

修复后「重试到上限」不再是丢弃，而是进 `video.timeline.update.queue.dlx`：
消息体、重试次数、失败原因都在，**可以查、可以人工重投**。这比静默 Ack 丢弃强在「问题可见」。

补偿路径有三个：
1. **从死信队列重投**：把死信消息重新投回主交换机（幂等，重复无害）；
2. **下次 ZSET 重建**：冷启动或 ZSET 被清空时，重建逻辑会从 MySQL 拉最新 1000 条，把它带回来；
3. **不补偿**：如果这个视频已经滚出最近 1000 条了，它本来就不该在热区里，补不补无所谓。

**加分点**：这体现了「按数据的丢失代价分层处理」——源数据必须零丢失（Outbox + confirm + 重试 + 终态告警），派生数据可以容忍丢失（有限重试 + 自动重建）。**不是所有数据都值得用同一套可靠性机制**，能说清这个分层是架构能力的体现。

---

### Q18：这个模块你怎么观测和告警？

**要点**：Outbox 的最大优势就是状态在数据库里，一条 SQL 就能观测。

**完整回答**：
基于 `outbox_msgs` 表就能做出核心指标（仓库里已经备了 `CountPending`）：

| 指标 | 查询 | 告警阈值 |
|---|---|---|
| 积压量 | `COUNT(*) WHERE status='pending'` | 持续增长超过 N 分钟 → poller 或 MQ 有问题 |
| 卡死量 | `COUNT(*) WHERE status='processing' AND claimed_at < now-60s` | > 0 持续 → 有实例崩溃或处理卡住 |
| 失败量 | `COUNT(*) WHERE status='failed'` | > 0 → 立刻告警，需人工介入 |
| 最老积压时长 | `MIN(create_time) WHERE status='pending'` | 超过阈值 → 投递链路断了 |
| 峰值重试次数 | `MAX(retry_count) WHERE status='pending'` | 接近 5 → 即将转 failed |

消费端还可以记「丢弃计数」（重试 3 次后丢弃的次数），这个指标持续大于 0 说明 Redis 有问题。

**加分点**：把「失败」显式建模成 `failed` 终态而不是「重试几次然后静默丢弃」，是为了**让问题可见**。静默丢弃是可靠性设计里最坏的一种——你连它发生过都不知道。

---

### Q19：这个模块还有哪些已知不足？

**要点**：诚实列出，并给出改进方向。

**完整回答**（主动暴露问题比被追问出来好得多）：

1. **`failed` 状态没有运维出口**：目前只能 `SELECT` 手工查，缺后台管理接口和告警。
2. **退避没有 jitter**：多实例同时重试会形成尖峰，应该加 ±30% 随机抖动打散。
3. **消费端重试是进程内阻塞**：`sleep(1s/2s/4s)` 会卡住该消费者整条流（QoS=10）。要优雅就得上延迟队列（TTL + DLX）。
4. **游标不够稳**：`ListLatest` 只用 `create_time`，同毫秒多条时可能漏项，应改成 `(create_time, id)` 复合游标（热榜已经是这么做的）。
5. **confirm 匹配是简化版**：串行前提下的「取下一条确认」，若要并发发布需改成 `DeferredConfirmation` 精确匹配 DeliveryTag。
6. **删除视频不会从 ZSET 移除 member**：被删视频的 ID 仍留在时间线里，靠 `GetVideoByIDs` 回源时过滤掉，不影响正确性，但会轻微影响当页条数。
7. **死信队列缺运维出口**（DLX 绑定错误已修）：所有 `<queue>.dlx` 曾用 `#` 绑到同一个 DLX exchange，死信会被广播到所有 DLX 队列 —— 已改成精确路由键。但 DLQ 目前只打日志，还缺告警和按 `event_id` 重投的管理接口。
8. **回收（ReapStuck）不区分「崩溃」与「慢」**：现在靠 60s 租约一刀切，慢的实例会被误回收导致重复投递。生产做法应该是租约续期（处理中定期刷新 `claimed_at`）。

---

### 一句话总结（如果被要求收尾）

> 这个模块的核心不是「用了 Outbox」，而是三件事：**用同库事务把跨系统原子性降维**、**用幂等把不重复投递降级成重复无害**、**用显式状态机把失败变成可观测可干预**。剩下的 confirm、CAS、退避、自愈，都是这三件事在工程上的落地。

---

## 12. 进阶追问

---

### 12.1 轮询对 DB 有压力，比较好的优化方向是什么？

**第一步：先量化，别急着优化。**

单实例「每秒一次空 SELECT」对 MySQL **几乎不构成压力**——走 `(status)` 索引定位、返回空，耗时微秒级，1 QPS 可忽略。

真正的代价在四处：

| 真痛点 | 说明 |
|---|---|
| **每条消息的写放大** | 一条消息 3 次 DB 往返：`Claim` → 发 MQ → `MarkSent`。批量 100 条 = 200 次 UPDATE |
| **多实例线性放大** | N 个 API 实例就是 N 倍空轮询，且 99% 是空转 |
| **积压时索引失效** | `next_retry_at IS NULL OR next_retry_at <= ?` 的 `OR` + `NULL` 很难走索引范围扫描；积压 10 万条都在退避期时，每次要逐行过滤才凑够 100 条 |
| **表膨胀** | `failed` 堆积同样拖慢扫描（已用 `PurgeSent` 部分缓解） |

**第二步：按 ROI 排序的优化方向**

| 方案 | 做法 | 收益 | 代价 |
|---|---|---|---|
| **A. 自适应轮询** | 查到 0 条就指数放大间隔（1s→2s→4s→封顶 5s），查到数据立刻回落 | 空闲期 DB 查询降到 1/5，多实例同比例放大收益 | 无（延迟上界变 5s，但没消息时空转本就无价值） |
| **B1. 批量 Claim** | 短事务里 `SELECT ... FOR UPDATE SKIP LOCKED` + 一条批量 UPDATE | 100 次 UPDATE → 2 次 SQL | 需 MySQL 8 |
| **B2. 批量确认** | 一批发完统一 `UPDATE ... WHERE id IN (...)`，失败的行单独 `MarkRetry` | 再省一半往返 | 要处理部分成功 |
| **B3. confirm 流水线** | 连发 N 条再统一收 N 个 ACK | 把 N×RTT 压成 1×RTT | 必须放弃「取下一条确认」的简化，改用 `map[DeliveryTag]` 精确匹配 |
| **C. 查询优化** | 用 `visible_at` 哨兵值（默认 0）替掉 `OR ... IS NULL`，建 `(status, visible_at, id)` 复合索引 | 从逐行过滤变索引范围扫描，积压越重收益越大 | 字段语义微调 |
| **D. id 水位线** | 记 checkpoint，`id > lastMaxID` 增量拉 | 不重复扫历史 | 需拆两条路：增量流 + 重试到期流 |
| **E. Redis 信号** | 业务提交后 `LPUSH outbox_signal`；poller 用 `BRPOP` 阻塞，有信号才查库 | DB 压力≈0，毫秒级延迟 | 信号会丢，必须有兜底慢轮询（30s 全量扫） |
| **F. CDC** | Canal/Debezium 订阅 binlog 推送 | 毫秒级、空闲零开销 | 新组件 + 位点 + 高可用 + 排障 |

**✗ 不推荐的**：为 Outbox 分库分表。表靠 `PurgeSent` 一直很小，行数与**投递速率**相关而非历史总量，分表解决的是错误的问题。

**本项目路线图**：

| 阶段 | 动作 | 触发条件 |
|---|---|---|
| 现在 | A + C | 无条件，成本极低 |
| 发布量上来 | B1 + B2（事务保持短） | 单批处理成瓶颈 |
| 再往后 | B3（DeliveryTag 精确匹配） | 单条 RTT 成瓶颈 |
| Redis 已重度使用 | E（Redis 信号 + 30s 兜底） | 想压到近 0 |
| 规模化 | F（CDC） | 秒级延迟不可接受 |

> **A 是唯一「零成本纯赚」的一条，优先级最高。** 对一个发布 QPS 个位数的项目引入 CDC 是典型过度设计——它解决的是不存在的瓶颈，却新增了一类故障源。

**顺带修正一个之前的判断**：当时 reject `SKIP LOCKED` 是因为它会把「发 MQ」包进事务。但批量化之后**事务里没有任何网络 I/O**，只有两条 SQL 的毫秒级窗口——这种情况下 `SKIP LOCKED` 比逐条 CAS 更优。
> 判断标准是**临界区里有没有不可控 I/O**，不是哪种语法更高级：长事务用 CAS，短事务用 SKIP LOCKED。

---

### 12.2 现在有 CAS 了，为什么还不能保证不重复投递？

**要点：CAS 保证的是「并发唯一」，不是「全局唯一」。**

看投递的三步：

```go
claimed, _ := repo.Claim(ctx, msg.ID, now)   // ① [DB]   pending → processing
if !claimed { continue }

err = tmq.PublishVideo(pubCtx, ...)          // ② [MQ]   副作用！不可回滚

if err == nil {
    repo.MarkSent(ctx, msg.ID, time.Now())   // ③ [DB]   processing → sent
}
```

- **CAS 只覆盖 ①**：保证同一时刻只有一个实例能把这行从 `pending` 改走。
  - 改前：3 个实例同时 `SELECT` 到同一批 pending → **每条消息稳定投递 3 次**
  - 改后：只有一个拿到 `RowsAffected=1`，其余跳过

- **但它挡不住「时序重复」**：② 的副作用不可逆——消息已经进了 broker。③ 是唯一记录「这件事发生过」的地方。**②③ 之间崩溃，就不知道到底发没发，只能重发。**

同源的四个漏洞：

| 场景 | 为什么 CAS 挡不住 |
|---|---|
| ② 成功、③ 前进程崩溃 | 行卡在 `processing`，60s 后被 `ReapStuck` 回收 → 重发 |
| confirm 超时但消息其实落盘 | ACK 丢失或 5s 定时器先触发。**超时 ≠ 失败**，只能按失败重试 |
| `ReapStuck` 误伤 | 实例 A 没死只是 GC 停顿超 60s；B 回收后重发，A 也发了一遍 |
| Claim 后、Publish 前崩溃 | 同上，等待回收后重发 |

**那 CAS 是不是白做了？** 不是。两者防的不是同一量级的重复：

| | 没有 CAS | 有 CAS |
|---|---|---|
| 重复频率 | 每条 × N 实例，**100% 发生** | 仅崩溃/超时窗口，**概率极低** |
| MQ 带宽浪费 | N 倍，随实例数线性增长 | 可忽略 |

只靠幂等扛 N 倍重复，规模化后是灾难。CAS 的价值是**把幂等从「主要防线」降级为「兜底保险」**。

**核心区分**：

```
投递语义（delivery）：at-least-once  ← 无法更好，除非 2PC
效果语义（effect）  ：exactly-once   ← 靠幂等实现 ✅
```

本模块的效果语义就是 exactly-once，因为 `ZADD` 同一 member 是覆盖。所以完整答案是三层：CAS 把重复压到极低频 → 幂等消化残余 → **端到端效果等价于 exactly-once，而只付 at-least-once 的成本**。

---

### 12.3 ZSET 的 score 里存的到底是什么？

**`videos.create_time` 的毫秒时间戳（Unix 毫秒）。**

全链路取值：

```
videos.CreateTime                                   ← 视频创建时间（MySQL datetime）
   ↓ video_service.go:53-58  写进 OutboxMsg
OutboxMsg.CreateTime
   ↓ outboxworker.go:114     tmq.PublishVideo(pubCtx, msg.VideoID, msg.CreateTime)
TimelineEvent.CreateTime
   ↓ timelineMQ.go:82        CreateTime: createTime.UnixMilli()   ← 转毫秒
   ↓ outboxworker.go:257     Score: float64(event.CreateTime)
ZSET score
```

ZSET 实际形态：

```
feed:global_timeline
  1761300000000  →  "987"     ← 最老（watermark）
  1761300001000  →  "988"
  1761300005000  →  "989"     ← 最新
```

**score = 发布时间，member = 视频 ID**，ZSET 按 score 升序，天然就是「按发布时间排序的时间线 ID 列表」。

**为什么用 `create_time`？因为它不可变。**

这是幂等的前提：如果 score 存的是会变的字段（点赞数、热度分），重复 `ZADD` 会让 score 漂移，同一条消息在不同时刻写入的排序结果不同，「重复无害」就不成立。

（对比：热榜的 `popularity` 是累加的，所以用 `ZINCRBY` 而非 `ZADD`，它天生不幂等
—— 保护方式见 `滑动窗口热榜与快照分页.md` §3.2.1：`event_id` + Lua 原子去重。）

**为什么用毫秒不用秒？**

1. **同秒碰撞 → 排序错误**：score 相同时 ZSET 按 member **字典序**排列，`"10" < "9"`，顺序就错了。毫秒把碰撞概率压低三个数量级。
2. **游标被"吃掉"**：游标分页用 `score < reqTime`，秒级精度下同秒的最后一条会把同秒其他条目全部排除，翻页直接漏数据。

**float64 精度边界**：Redis ZSET score 是 IEEE 754 double，53 位有效位 ≈ `9.007e15`

| 单位 | 当前量级 | 是否安全 |
|---|---|---|
| 秒 | ~1.7e9 | ✅ |
| 毫秒 | ~1.7e12 | ✅（约 5000 倍余量，撑到公元 2286 年） |
| 微秒 | ~1.7e15 | ⚠️ 临界 |
| 纳秒 | ~1.7e18 | ❌ 精度丢失 |

**score 的四个用途**

| 用途 | 代码 |
|---|---|
| **排序取最新** | `ZREVRANGEBYSCORE key maxScore -inf LIMIT 0 limit` |
| **裁剪 1000 条** | `ZREMRANGEBYRANK key 0 -1001`（保留 score 最大的 1000 条） |
| **冷热水位线** | `watermark := int64(zsetTail[0].Score)` |
| **游标分页** | `maxScore = reqTime - 1`，严格小于防重复 |

注意：**热区是「最近 1000 条」而不是「最近 N 小时」**，容量按条数封顶，所以 `watermark` 是随发布速率浮动的时间点——发布越频繁，热区覆盖的时间窗口越短。

---

## 13. 本次改动清单

| # | 文件 | 改动 |
|---|---|---|
| 1 | `rabbitmq/timelineMQ.go` | 开启 publisher confirm + `waitConfirm` + `Close()` + `DeclareTimelineTopic()` |
| 2 | `video/video_entity.go` | 新增状态常量、`RetryCount / NextRetryAt / ClaimedAt / SentAt` 字段、`OutboxBackoff()` |
| 3 | `video/outbox_repo.go` | 新增：`ListDue / Claim / MarkSent / MarkRetry / MarkFailed / ReapStuck / PurgeSent / CountPending` |
| 4 | `worker/outboxworker.go` | poller 重写（CAS + 状态机 + 自愈 + 退避）；consumer 重写（拓扑声明 + 有限重试）；新增 `writeTimeline / zaddTimeline / sleepOrDone` |
| 5 | `http/router.go` | 改为注入 `OutboxRepository`，poller/consumer 各自内部按需建连 |
| 6 | `项目亮点（简历写法）.md` | 修正「强一致」「Worker 独立消费」等过度宣称的表述 |

> 数据库字段通过 `db.AutoMigrate(&video.OutboxMsg{})` 自动加列，历史 `pending` 行会被正常接管。

### 第二轮改动（幂等键 + 崩溃接管 + 消费端容错）

| # | 文件 | 改动 |
|---|---|---|
| 7 | `video/video_entity.go` | `OutboxMsg` 新增 `EventID`（唯一索引 `idx_outbox_event_id`）与 `ClaimToken`（fencing 令牌） |
| 8 | `video/video_service.go` | 写 outbox 的同一事务里生成 `event_id` 并落库 |
| 9 | `video/outbox_repo.go` | `Claim` 返回 fencing token 并写入 `claim_token`；`MarkSent/MarkRetry/MarkFailed` 全部校验 token；`ReapStuck` 改为逐条处理、清 token、计入重试额度、超限转 `failed` |
| 10 | `rabbitmq/timelineMQ.go` | `PublishVideo` 改为**接收**外部 `event_id`（不再内部生成），保证重投不换 ID |
| 11 | `worker/outboxworker.go` | poller 传 `msg.EventID`、处理 token 失效；`StartConsumer` 改用统一 `Consumer` 骨架；删除 `writeTimeline` 内联重试 |
| 12 | `rabbitmq/topics.go` | 新增 `TimelineTopic`，补齐时间线的重试队列 + 死信队列拓扑 |
| 13 | `http/router.go` | 为时间线与 3 个通知队列创建 `RetryRouter`；新增 `startDLQConsumers` 消费死信队列 |

> ⚠️ 上线注意：主队列新增了 `x-dead-letter-routing-key` 参数，已存在的队列会报
> `PRECONDITION_FAILED (406)`，需要先删除旧队列（或改名）。

---

## 14. 还没做的（下一步）

按优先级：

1. **`failed` 状态缺少运维出口**：目前只能靠 `SELECT * FROM outbox_msgs WHERE status='failed'` 人工查，建议加一个后台管理接口或告警。
2. **退避无 jitter**：多实例同时重试会产生尖峰，可加 ±30% 随机抖动。
3. **进程内退避重试仍会阻塞该消费者**：`Consumer` 骨架里 1s/2s/4s 的退避会卡住这条流（时间线 QoS=10）。已有 TTL 重试队列兜底，但更优做法是降低第一级重试次数、尽早转重试队列。
4. **游标不够稳**：`ListLatest` 只用 `create_time`，建议改成 `(create_time, id)` 复合游标，与热榜对齐。
5. **死信队列缺运维出口**：DLX 绑定错误已修（精确路由键），但 DLQ 目前只打日志，还缺告警和按 `event_id` 重投的管理接口。
6. **回收不区分「崩溃」与「慢」**：60s 租约一刀切，慢的实例会被误回收导致重复投递。生产应做租约续期。
7. **`failed` 状态缺运维出口**：只能 `SELECT` 手工查，缺后台管理接口和告警。
8. **退避无 jitter**：Outbox 的 `OutboxBackoff` 是纯指数退避，多实例同时重试会形成尖峰（消费端 `Consumer.backoff` 已加 jitter，poller 还没加）。
