# 01 · 冷热分离时间线 + Outbox（专题）

> 配套 Q&A：`11-冷热分离时间线-面试QA.md`
> 全局索引与代码锚点总表：`00-索引-面试文档地图.md`
> **行号基准：2026-10-27**

> 覆盖链路：`发布视频` → `Outbox 本地消息表` → `RabbitMQ` → `Redis ZSET 热时间线` → `Feed 冷热分离查询`
>
> 本文配合代码阅读。文中「改前 / 改后」指两轮可靠性修复：第一轮是 confirm、状态机、poller 自愈、消费端有限重试；第二轮是稳定 `event_id`、fencing token、崩溃接管、三级容错。
>
> **第三轮同步（当前实现）**：发布端退避加 ±30% 抖动、broker/连接级故障与消息级失败分流（`MarkDeferred`）、`PublishWithDeferredConfirm` 按 `DeliveryTag` 精确等待、`last_error` 落库、`PurgeSent` 分批删除、周期 `Stats` + `[ALERT]` + `/metrics` gauge、`/admin/outbox/*` 查询与重放接口。文末「还没做的」已按现状重写。

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

```50:68:backend/internal/video/video_service.go
	//事务保证视频写入库和消息写入本地消息表的一致性
	err := vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(video).Error; err != nil {
			return err
		}

		// 幂等键必须和业务数据同事务落库，重投时才能保持同一个 event_id
		eventID, err := rabbitmq.NewEventID()
		if err != nil {
			return err
		}
		msg := OutboxMsg{
			VideoID:    video.ID,
			EventID:    &eventID,
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
│        │ ③ PublishVideo : 发 MQ + 按 DeliveryTag 等 confirm       │
│        │ ④ MarkSent / MarkDeferred / MarkRetry / MarkFailed      │
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
│        │ Lua 原子执行：                                          │
│        │   ZADD feed:global_timeline score=create_time(ms)       │
│        │                              member=video_id            │
│        │   ZREMRANGEBYRANK 0 -(capacity+1)  ← 只保留最近 1000 条  │
│        │   ZCARD                                                  │
│        ▼                                                        │
│  Redis ZSET: feed:global_timeline   ← 这就是「热时间线」         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│  FeedService.ListLatest(ctx, limit, cursor, viewerAccountID)     │
│    取 ZSET 最老一条的 score 作为 watermark                       │
│    cursor == nil 或 reqTime > watermark → 热区：Redis ZSET       │
│    reqTime <= watermark → 冷区：MySQL 复合游标                   │
│    热区不足 want(=limit+1) → 用最后一条 (create_time,id) 缝合    │
│    has_more = 冷热合并后是否超过 limit 条                          │
└─────────────────────────────────────────────────────────────────┘

后台任务：
  每 5 分钟对账 MySQL 最新 1000 条 vs ZSET，只补缺失 member；
  删除视频时删库 + 失效实体缓存 + ZREM 热时间线。
```

**一句话概括**：写入侧用 Lua 保证「加一条 + 裁一条」原子；读取侧用 `(create_time,id)` 复合游标把 Redis 热区和 MySQL 冷区串成一条有序流；`has_more` 靠多取一条独立探测；丢失靠重建 + 对账自愈。

---

## 2. 代码地图

| 文件 | 职责 |
|---|---|
| `backend/internal/video/video_entity.go` | `OutboxMsg` 结构体 + 状态常量 + 退避计算 |
| `backend/internal/video/outbox_repo.go` | Outbox 表的全部 SQL（查询/抢占/状态流转/回收/清理） |
| `backend/internal/video/timeline.go` | 热时间线 key 与容量常量 `TimelineCapacity = 1000` |
| `backend/internal/video/video_service.go` | 发布视频（业务数据 + Outbox 同事务）；删除视频时 `ZREM` 热时间线 |
| `backend/internal/worker/outboxworker.go` | `StartOutboxPoller`（投递 + 周期告警）+ `StartConsumer`（Lua 原子写 ZSET） |
| `backend/internal/middleware/rabbitmq/timelineMQ.go` | 发布端封装，`PublishWithDeferredConfirm` 精确 confirm 就在这里 |
| `backend/internal/http/outbox_admin.go` | `/admin/outbox/stats|failed|replay` 运维接口 |
| `backend/internal/observability/` | `/metrics` 指标（`feedsystem_outbox_*`） |
| `backend/internal/feed/service.go` | `ListLatest` 冷热分离查询、空 ZSET 重建、5 分钟对账 |
| `backend/internal/feed/repo.go` | 冷数据复合游标查询 |
| `backend/internal/middleware/redis/zset.go` | ZSET 封装（`ZRem` 用于删除清理） |

---

## 3. Outbox 状态机

```
                    ┌──────────────────────────────────────────────┐
                    │                                              │
                    ▼                                              │
              [ pending ] ──Claim(CAS)──> [ processing ]          │
                    ▲                          │                  │
                    │                          │                  │
                    │ MarkRetry                ├─ 成功 ─> [ sent ] ──24h──> 删除
                    │ (retry_count+1,          │
                    │  指数退避+抖动)           └─ 重试耗尽 ─> [ failed ]
                    │                                              (终态, 等人工)
                    │ MarkDeferred
                    │ (broker 级故障, retry_count 不变, 5s 后可见)
                    │
              ReapStuck: claimed_at 超时(60s) 未落地 → 回收回 pending
                       （处理进程崩溃 / 假死；同样消耗一次重试额度）
```

三个回退到 `pending` 的入口，语义不同：

| 入口 | 触发 | 是否消耗重试额度 | 下次可见 |
|---|---|---|---|
| `MarkRetry` | 消息级投递失败 | 是（+1） | 指数退避 + ±30% 抖动 |
| `MarkDeferred` | broker/连接级故障（channel 关闭、I/O 失败、confirm 超时） | **否** | 固定 5s |
| `ReapStuck` | `processing` 超过 60s 未落地 | 是（+1），超限转 `failed` | 立即 |

### 为什么需要 `processing` 这个中间态？

如果只有 `pending / sent` 两态，多实例部署时两个 poller 会**同时查到同一批 pending 消息并各发一遍**。中间态 `processing` 就是一把「谁先改谁得」的锁。

### 为什么用 CAS 而不是 `SELECT ... FOR UPDATE SKIP LOCKED`？

```go
// Claim 抢占一条消息：只有把 status 从 pending 改成 processing 的那个实例拿到发送权。
// 返回的 token 是 fencing 令牌，后续 MarkXxx 必须原样带回。
func (r *OutboxRepository) Claim(ctx context.Context, id uint, now time.Time) (token string, ok bool, err error) {
	token, err = newClaimToken()
	if err != nil {
		return "", false, err
	}
	tx := r.db.WithContext(ctx).Model(&OutboxMsg{}).
		Where("id = ? AND status = ?", id, OutboxStatusPending).
		Updates(map[string]interface{}{
			"status":      OutboxStatusProcessing,
			"claimed_at":  now,
			"claim_token": token,
		})
	if tx.Error != nil {
		return "", false, tx.Error
	}
	if tx.RowsAffected != 1 {
		return "", false, nil // 已被其他实例抢走
	}
	return token, true, nil
}
```

状态迁移统一走 `updateWithToken`：

```go
Where("id = ? AND status = ? AND claim_token = ?", id, OutboxStatusProcessing, token).
	Updates(values)
```

`RowsAffected=1` 才算生效；旧持有者恢复后拿着过期 token 更新会命中 0 行，只能记日志放弃。

`SKIP LOCKED` 需要把「发消息到 RabbitMQ」这段网络 I/O 包在数据库事务里——事务会长达几十毫秒，锁竞争很痛。CAS 只在**一条 UPDATE** 上加行锁，拿到发送权之后再慢慢发。**安全性等价，性能更好**，这是面试可以说的一个点。

### 为什么需要 `ReapStuck`？

实例 A 抢占了消息（`processing`），还没发出去就崩了。这条消息会永远卡在 `processing`，没人再管它。所以需要有回收逻辑：`claimed_at` 超过 60s 还没落地，就放回 `pending`。

### 退避：`NextRetryAt` 解决忙轮询

```go
func OutboxBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 0
	}
	shift := retryCount - 1
	if shift > 10 { // 防止移位溢出
		shift = 10
	}
	base := OutboxBaseBackoff * time.Duration(1<<uint(shift)) // 2s, 4s, 8s, 16s, 32s
	delta := base * 3 / 10                                    // ±30%
	return base - delta + time.Duration(rand.Int63n(int64(2*delta)))
}
```

名义值仍是 `2s × 2^(n-1)`，实际区间为
`1.4~2.6 / 2.8~5.2 / 5.6~10.4 / 11.2~20.8 / 22.4~41.6s`。

**为什么要 ±30% 抖动**：多实例部署时，各实例的退避相位会趋同；broker 恢复的瞬间它们会一起重试，形成二次尖峰。±30% 足以打散相位，同时相邻退避区间不重叠，不破坏「次数越多等越久」的指数性质。

> broker/连接级故障不走这条路径，而是 `MarkDeferred`（5s 后可见、`retry_count` 不变），详见 §5。

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
// 通道开启 confirm 模式（Confirm(false) = 非阻塞，Confirm 调用本身不等 broker）
if err := ch.Confirm(false); err != nil { ... }

// 每条消息拿到自己的 DeferredConfirmation，按 DeliveryTag 精确等待
dc, err := ch.PublishWithDeferredConfirm(exchange, rk, false, false, amqp.Publishing{
	ContentType:  "application/json",
	DeliveryMode: amqp.Persistent,
	Body:         body,
})
if err != nil {
	return fmt.Errorf("%w: publish: %v", ErrBrokerUnavailable, err)
}

waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
acked, err := dc.WaitContext(waitCtx)
if err != nil {
	return fmt.Errorf("%w: confirm timeout: %v", ErrBrokerUnavailable, err)
}
if !acked {
	if ch.IsClosed() { // channel 关闭时库会把所有未决确认置为 NACK
		return fmt.Errorf("%w: channel closed before confirm", ErrBrokerUnavailable)
	}
	return fmt.Errorf("%w: delivery tag %d", ErrPublishRejected, dc.DeliveryTag)
}
return nil
```

失败分三类：

| 失败 | 分类 | Outbox 的处理 |
|---|---|---|
| channel 关闭、publish I/O 失败、confirm 5s 超时 | `ErrBrokerUnavailable`（基础设施故障） | `MarkDeferred`：5s 后重试，**不消耗重试额度**；重建 channel |
| channel 仍存活时 broker 明确 NACK | `ErrPublishRejected`（消息级失败） | `MarkRetry`：指数退避 + 抖动，5 次后转 `failed` |
| 消息构造失败（videoID 为空、JSON 序列化失败） | 普通 error | 按消息级失败处理 |

> 早期实现用 `NotifyPublish` + 「取下一条确认」，靠 `PublishVideo` 串行调用来保证不错位。现在已改成 `DeferredConfirmation` 按 `DeliveryTag` 精确匹配：迟到的 ACK 只会落在自己那条消息上，即使将来改成并发发布也不会串。

---

## 5. 修复 ②：状态机取代「发布即删除」

改前是 `Delete(&msg)`，没有任何重试语义；改后是一条完整的流水线：

```go
token, claimed, err := repo.Claim(ctx, msg.ID, now) // 抢占，返回 fencing token
if !claimed { continue }                             // 被别人抢走，跳过

eventID := ""
if msg.EventID != nil { eventID = *msg.EventID }
pubErr := tmq.PublishVideo(pubCtx, msg.VideoID, msg.CreateTime, eventID) // 含 confirm

if pubErr == nil {
	repo.MarkSent(ctx, msg.ID, token, time.Now())   // 失败/令牌失效只记日志
	continue
}

// 基础设施故障：消息本身没问题，不烧重试额度
if errors.Is(pubErr, rabbitmq.ErrBrokerUnavailable) {
	repo.MarkDeferred(ctx, msg.ID, token, time.Now(), video.OutboxBrokerRetryDelay, errText(pubErr))
	_ = tmq.Close(); tmq = nil                       // 大概率整条 channel 坏了，重建
	break                                            // 跳出本批，别撞同一条坏连接
}

// 消息级失败：消耗重试额度，耗尽后转 failed
if msg.RetryCount+1 > video.OutboxMaxRetries {
	repo.MarkFailed(ctx, msg.ID, token, errText(pubErr))
	continue
}
repo.MarkRetry(ctx, msg.ID, token, msg.RetryCount+1, time.Now(), errText(pubErr))
// 不重建 channel：NACK 是这条消息的问题，不代表连接坏了
```

两个关键点：

1. **只有 broker/连接级故障才重建 channel 并跳出本批**。旧实现「任何失败都 Close + break」会把一条坏消息升级成整批延迟；
2. **`MarkDeferred` 不增加 `retry_count`**。否则「MQ 停机 2 分钟」就足以把积压消息全部烧成 `failed`——基础设施故障被误判成消息问题。

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

运行中 channel 失效走另一条路：`PublishVideo` 返回 `ErrBrokerUnavailable` → `MarkDeferred`（5s 后可见、不烧额度）→ 关闭并重建 channel。因此「MQ 半死不活」也不再会把消息烧成 `failed`。

> 永久性拓扑损坏（例如交换机被误删，broker 关闭 channel）会表现为持续的 `ErrBrokerUnavailable`，消息一直留在 `pending`。这是刻意取舍：宁可让消息等着，也不把可能正确的消息判死；由「最老 pending > 5min」的 `[ALERT]` 兜底。

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
	// 时间线跑在 QoS=10 的单条流上，进程内 sleep 重试会阻塞整条流
	MaxLocalRetries: 1,
	Process: func(ctx context.Context, d amqp.Delivery) error {
		return processTimeline(ctx, redisClient, d)
	},
}
```

```
写 ZSET 失败
  → 进程内 1 次退避重试（默认骨架是 1+3 次；时间线用 MaxLocalRetries=1）
  → 仍失败：投到 video.timeline.update.queue.retry，TTL 30s 后自动重投（最多 3 轮）
  → 仍失败：Nack(requeue=false) → video.timeline.update.queue.dlx 死信队列
       → DLQWorker 落 dead_letters 表（(queue,event_id) 唯一 + INSERT IGNORE）+ 指标 + 日志
```

**为什么时间线只留 1 次本地重试**：进程内 `sleep` 是串行阻塞的，QoS=10 的流会被一条失败消息拖住。把重试尽快交给 TTL 重试队列，既释放消费者，又保留 at-least-once 语义。

**为什么改掉了「3 次后 Ack 丢弃」**：那是把 Outbox 辛辛苦苦保证的 at-least-once
在最后一环丢掉了，而且时间线少一条**没有任何报错**，是最难排查的一类故障。
现在最坏情况是消息躺在死信队列里等人处理，而不是消失。

> 代价：Redis 长时间不可用时，死信队列会持续堆积。这是刻意的——**宁可堆积可见的失败，
> 也不要静默丢弃**。详见下一节和 8.5 节。

---

## 8. 为什么「至少一次投递」不会造成脏数据？

因为 ZSET 的 member 是 `video_id`，重复 `ZADD` 同一个 member 只是**覆盖 score**，不会重复出现：

```go
// zaddTimeline 单次写入。ZSET 的 member 是 video_id，重复写入是幂等覆盖，
// 所以"至少一次投递"不会造成脏数据——这正是 Outbox 可以放心重试的前提。
func zaddTimeline(ctx context.Context, redisClient *rediscache.Client, event *rabbitmq.TimelineEvent) error {
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// 写入与裁剪必须原子：分两条命令时中间会露出 1001 条的窗口
	_, err := redisClient.Eval(opCtx, timelineZAddAndTrim, []string{video.TimelineZSetKey(redisClient)},
		float64(event.CreateTime), fmt.Sprintf("%d", event.VideoID), video.TimelineCapacity)
	return err
}

const timelineZAddAndTrim = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[3]) + 1))
return redis.call('ZCARD', KEYS[1])
`
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
| 1 | `MarkSent/MarkDeferred/MarkRetry/MarkFailed` 只校验 `status='processing'`，**不校验归属** | Worker A 卡住 61s 被回收 → B 抢占 → A 恢复后调 `MarkSent` **把 B 还没发成功的消息标成 sent** → 消息永久丢失 | **fencing token**：`Claim` 生成随机令牌写进 `claim_token`，所有状态迁移（含 `MarkDeferred`）都带 `WHERE claim_token = ?`；旧持有者拿到 `ok=false` 时只记日志、不动状态 |
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

> 本节已按 2026-10-27 的实现重写。旧版本里的 `latestBefore time.Time`、`maxScore = reqTime-1`、只缝合 `CreateTime` 都已过时。

### 9.1 写入：Lua 原子写入 + 裁剪

消费端不再分两条命令执行 `ZADD` 和 `ZREMRANGEBYRANK`，而是一次 `EVAL`：

```go
// worker/outboxworker.go
_, err := redisClient.Eval(opCtx, timelineZAddAndTrim,
    []string{video.TimelineZSetKey(redisClient)},
    float64(event.CreateTime),                  // score = create_time(ms)
    strconv.FormatUint(event.VideoID, 10),      // member = video_id
    video.TimelineCapacity,                     // 1000
)
```

```lua
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[3]) + 1))
return redis.call('ZCARD', KEYS[1])
```

三个要点：

1. **原子性**：脚本执行期间 Redis 不会插入其他命令，不存在「刚加完还没裁」的 1001 条窗口；
2. **容量只有一处定义**：`video.TimelineCapacity = 1000`，写入裁剪、空 ZSET 重建、后台对账共用，避免「重建完立刻被裁」；
3. **不是事务回滚**：Lua 保证不被穿插，但运行时错误不会撤销已执行的 `ZADD`。正常路径的命令类型固定，不会触发；真出错时靠 5 分钟对账把基数裁回上限。

### 9.2 水位线与分支

**第一步：取水位线。** `ZRANGE key 0 0 WITHSCORES` 拿到 ZSET 里**最老**那条（ZSET 按 score 升序，index 0 最小），它的 score 就是 `watermark`。

**第二步：比游标。** 游标是 `TimelineCursor{CreateTime, ID}`，不是单个时间：

```
cursor == nil                 → 首页，必在热区
reqTime > watermark           → 热区：ZREVRANGEBYSCORE
reqTime <= watermark          → 冷区：MySQL 整页
```

为什么 `==` 必须走冷区：`ZREMRANGEBYRANK` 按**排名**裁剪，可能在最小 score 的分组内部切断，热区最老那一毫秒的记录不完整。游标一旦严格大于 watermark，该分数组必然完整（反证：若有成员被裁，它的 score 只会 ≤ watermark）。

### 9.3 热区取数：上界含游标 + 应用层重排

旧实现用 `maxScore = reqTime - 1`，会把游标所在毫秒的其余视频整体排除——这是漏项的根因。现在的做法：

```
maxScore = reqTime（含）
count    = want + ZCOUNT(key, reqTime, reqTime)   ← 补上同毫秒兄弟，避免占满名额
ZREVRANGEBYSCORE key maxScore -inf LIMIT 0 count
→ 丢弃 score == reqTime && id >= cursor.ID
→ 按 (score DESC, id DESC) 重排
→ 截断到 want
```

**为什么必须应用层重排**：同一 score 下 Redis 按 member 的**字典序**排，`"10" < "9"`，和数值 id 顺序不一致；MySQL 侧是 `ORDER BY id DESC`，两边必须对齐。

**为什么取 `want = limit+1`**：多出的一条不返回给客户端，只用来判断 `has_more`。

### 9.4 缝合与 `has_more`

热区不足 `want` 条（跨过冷热边界，或热区里有视频被删除过滤）时，用**最后一条实际装配成功的视频**的复合游标去 MySQL 补：

```go
want := limit + 1

if len(baseVideos) < want {
    remainLimit := want - len(baseVideos)
    var coldCursor *TimelineCursor
    if len(baseVideos) > 0 {
        last := baseVideos[len(baseVideos)-1]
        coldCursor = &TimelineCursor{CreateTime: last.CreateTime, ID: last.ID}
    } else {
        coldCursor = cursor   // 热区为空时回退到入参游标，避免 nil
    }
    // 冷区查询失败直接返回错误，不能悄悄返回短页
}

hasMore := len(baseVideos) > limit
if hasMore {
    baseVideos = baseVideos[:limit]
}
```

两个容易答错的点：

1. **补一次就够**：冷区直接返回 MySQL 行，没有后置过滤（删除的视频在库里已不存在），所以「冷区返回条数 < `remainLimit`」只可能是真的到底了，不需要循环回源；
2. **`has_more` 与「页满没满」解耦**：服务端实际查了 `limit+1` 条，只是丢掉多的一条。即使热段被删除过滤掉几条，也会用冷段按缺口补；补不到才是真的到底。

**关键取舍**：补齐的冷数据**不回写 ZSET**。否则用户随便翻几页就能把历史数据灌进只保留 1000 条的热池，把真正的新视频挤出去——这就是「冷数据污染热点池」。

### 9.5 删除与自愈

- **删除视频**：删库 + 失效实体缓存 + 按 `video_id` `ZREM` 热时间线（100ms 预算，失败只记日志）。删不掉的残留靠 `GetVideoByIDs` 装配时过滤。
- **Redis 整体不可用**：`rediscache == nil` 或 `ZRANGE` 报错，直接走 `listLatestFromDB`。
- **ZSET 为空**：singleflight 全局收敛，从 MySQL 拉最新 1000 条重建；DB 也为空时返回 `EMPTY_DB` 哨兵防递归。
- **ZSET 非空但少了几条**：5 分钟对账任务比较 MySQL 最新 1000 条与 ZSET member，**只补缺失、不做差集删除**（对账期间新发布的视频可能不在 MySQL 快照里，删「多余」会误伤）；补完统一按 `TimelineCapacity` 裁剪一次，维持上限。

### 9.6 游标分页 vs OFFSET

```sql
SELECT * FROM videos
WHERE (create_time < ?) OR (create_time = ? AND id < ?)
ORDER BY create_time DESC, id DESC
LIMIT ?;
```

- `OFFSET 100000` 要扫过前 10 万行再丢弃，越翻越慢；
- 复合游标直接 seek 到索引位置，**第 1 页和第 1000 页代价一样**；
- 期间有新视频插入时，`OFFSET` 会导致「上页末尾重复 / 某些条目漏掉」，游标不会；
- `idx_videos_create_time (create_time DESC)` 的叶子节点自带主键，逻辑上等价 `(create_time, id)`；但 `OR` 条件是否真的走单段 range，必须用 `EXPLAIN` 验证，不能靠推断。
- 单用 `create_time` 在同毫秒多条时不稳定，所以时间线现在和热榜一样使用 `(时间, id)` 复合游标（`TimelineCursor`）。

---

## 10. 失败场景演练（面试常问）

| 场景 | 行为 | 结果 |
|---|---|---|
| 发布时 MQ 抖动导致 confirm 超时 / channel 关闭 | `ErrBrokerUnavailable` → `MarkDeferred`（5s 后重试、不烧额度）→ 重建 channel | ✅ 最终投递成功 |
| broker 明确 NACK 这条消息 | `ErrPublishRejected` → `MarkRetry`（指数退避 + 抖动），5 次后转 `failed` | ✅ 消息级失败才会终态化 |
| RabbitMQ 整机宕机 5 分钟 | poller 每 5s 重建 TimelineMQ 失败，消息在表里积压；恢复后自动补发 | ✅ 不丢，延迟投递 |
| 进程在 `processing` 阶段崩溃 | 60s 后 `ReapStuck` 回收回 `pending`；进程重启后第一轮**立刻**回收 | ✅ 不丢 |
| 崩溃实例恢复后想改状态 | fencing token 校验失败（`claim_token` 已被刷新），只记日志 | ✅ 不会误标别人正在处理的消息 |
| 两个 API 实例同时轮询 | `Claim` 的 CAS 只有一个拿到 `RowsAffected=1` | ✅ 不重复投递 |
| 同一消息被重复投递两次 | ZSET member = video_id，覆盖写 | ✅ 幂等，无脏数据 |
| confirm 丢失但消息其实已落盘 | 按失败处理 → MarkRetry → 重投 | ✅ 消费端幂等，重复无害 |
| Redis 宕机期间消费消息 | 本地 1 次重试 → 重试队列重投 3 轮 → DLQ 落 `dead_letters` + 告警 | ⚠️ 该视频暂不在热时间线，冷区仍可查到；消息可按 `event_id` 查到 |
| ZSET 非空但少了几条 member | 空判定不触发；5 分钟对账比较 MySQL 最新 1000 条与 ZSET，只补缺失 | ⚠️ 最多延迟 5 分钟恢复；期间该视频冷区可查 |
| 视频删除后 `ZREM` 失败 | 删除不回滚；读路径 `GetVideoByIDs` 装配时过滤查不到的 ID；残留 member 会被后续写入的 rank 裁剪挤出 | ✅ 用户看不到已删视频；最多浪费几个热区名额，不影响正确性 |
| 消息重试 5 次仍失败 | 转 `failed` 终态 + `[ALERT]` 日志 | ⚠️ 可通过 `/admin/outbox/failed` 查看、`/replay` 重放 |

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
3. **表增长**：消息发成功不能删（否则没有审计和重试依据），所以改成 `sent` 状态 + 24 小时后 `PurgeSent` 清理。清理按 1000 条/批循环删除，避免一条大 `DELETE` 形成长事务和主从延迟。

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

**加分点**：confirm 有两种用法——同步等（本项目，每发一条用 `PublishWithDeferredConfirm` 等一次，按 `DeliveryTag` 精确匹配）和批量等（发 N 条后一起等，吞吐高但要处理失败重发）。时间线发布 QPS 很低，所以选了最简单也最精确的同步等。

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
写入时用一段 Lua 同时做 `ZADD` 和 `ZREMRANGEBYRANK key 0 -(capacity+1)`，只留 score 最大的 1000 条：

```lua
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[3]) + 1))
return redis.call('ZCARD', KEYS[1])
```

选 1000 的考虑：

- **内存可控**：一条 member 是数字 ID，1000 条在几十 KB 量级，即使未来按标签/分频道扩展多个 ZSET 也不心疼。
- **热区的定义**：绝大多数用户刷 Feed 不会翻过前几屏，1000 条覆盖绝大部分真实请求。
- **超出后的代价可接受**：翻出热区就走 `WHERE (create_time < ?) OR (create_time = ? AND id < ?) ORDER BY create_time DESC, id DESC LIMIT ?`，配合 `idx_videos_create_time`，代价也是索引 seek，不会随页码劣化。

真正的代价是**热区覆盖率有限**——如果站点发布量很大（比如每秒几十条），1000 条可能只覆盖几分钟，热区命中率就很低了。这种情况下应该按发布速率动态调容量，或者干脆改成「按时间而非按条数」裁剪。

**加分点**：容量必须是**一个可配置常量**而不是散落的魔法数字。这里抽成了 `video.TimelineCapacity = 1000`，写入裁剪、空 ZSET 重建、后台对账三处共用同一个值——任何一处漏改，都会出现「刚重建就被裁掉」或「对账补完又超量」的抖动。

---

### Q7：你最初的实现有什么问题？你是怎么发现并修的？

**要点**：四个缺陷——① 无 publisher confirm 会丢消息；② 无状态机多实例会重复投递；③ MQ 挂了 poller 永久退出不补发；④ 消费端无限 requeue 死循环。

**完整回答**（这是展示工程能力的最好机会，按「发现 → 定位 → 修复 → 残留」讲）：

1. **无 publisher confirm**：`PublishJSON` 发出就当成功，紧接着 `db.Delete(&msg)`。broker 未落盘时消息永久丢失且无法补回。**修复**：通道开启 confirm，收到 ACK 才置 `sent`。
2. **无状态机/抢占**：`SELECT * WHERE status='pending' LIMIT 100` 后直接发，多实例部署会各发一遍。**修复**：加 `processing` 中间态 + CAS 抢占 + `claimed_at` 崩溃回收。
3. **poller 一次性判空**：启动时 `tmq == nil` 就 `return`，MQ 恢复后积压消息永远不投递，必须重启 API。**修复**：改成循环内周期性重建，恢复即补发。
4. **消费端无限 requeue / 静默丢弃**：`Nack(false, true)` 无次数上限，Redis 故障期间消息被打成死循环；后来改成「重试 3 次后 Ack 丢弃」又走向另一个极端，把 at-least-once 在最后一环丢掉了。**修复**：复用统一 `Consumer` 骨架，改成三级容错（进程内退避 → 重试队列 TTL 重投 → 死信队列落表告警），全程不静默 Ack 失败消息。

**第三轮又补了四个**：发布端退避加 ±30% 抖动；broker/连接级故障分流到 `MarkDeferred`（不再烧重试额度）；confirm 改成 `DeferredConfirmation` 按 `DeliveryTag` 精确匹配；`failed` 增加周期统计、`[ALERT]`、`/admin/outbox/*` 查询与重放。这些不再是「最初实现的缺陷」，而是从「能跑」到「可运维」的补齐。

**第四轮修的是查询侧**：原子写入裁剪、复合游标、`has_more` 探测、缝合补数、删除 `ZREM`、5 分钟对账（详见第 9 节与第 13 节）。它们不在投递链路上，但同样是「文档里成立、代码里不成立」的反推结果——比如旧代码的注释写着「严丝合缝」，实际跨边界那页的 `has_more` 是错的。

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

**要点**：超时不再有「ACK 错位」问题——每条消息按 `DeliveryTag` 精确等待；但超时通常意味着连接级故障，重建 channel 是为了把后续消息从坏连接上挪开。

**完整回答**：
旧实现是 `NotifyPublish` + 「取下一条确认」：第 N 条超时后，迟到的 ACK 可能被第 N+1 条误认，所以必须重建 channel 来作废旧确认。这是当时的简化，前提是发布串行。

现在改成 `PublishWithDeferredConfirm`：每条消息拿到独立的 `DeferredConfirmation`，`WaitContext` 只等自己的 `DeliveryTag`。即使第 N 条的 ACK 迟到，也只会落在第 N 条上，不会污染第 N+1 条。

那为什么还要重建？两个原因：

1. confirm 超时 / publish I/O 失败 / channel 关闭都被归类为 `ErrBrokerUnavailable`，说明**连接本身可能已经不可用**。继续用同一条 channel 发本批剩余消息，只会连续失败；
2. channel 关闭时 amqp 库会把所有未决确认置为 NACK；重建可以避免把一堆已失效的确认留在内存里。

两者职责已经分开：

- **broker 级故障** → `MarkDeferred` + `Close()` + `break` 本批，下一轮重建；
- **消息级 NACK** → 只 `MarkRetry` 当前条，**不重建 channel**，继续处理本批。

**加分点**：精确匹配解决的是「确认归属」，重建解决的是「连接健康」，不是同一件事。旧答案把两者绑在一起，是因为旧实现依赖「串行 + 重建」规避错位；现在拆开了。

---

### Q11：游标分页和 OFFSET 到底差在哪？

**要点**：OFFSET 是「扫过并丢弃前 N 行」，越翻越慢；游标是「从索引位置直接开始」，每页代价相同。且 OFFSET 在并发写入下会重复/漏项。

**完整回答**：
`LIMIT 10 OFFSET 100000` 的执行过程是：先取出 100010 行，丢弃前 100000 行，返回 10 行。翻到第 10000 页时数据库要做十万行量级的无用功。

```sql
-- 复合游标写法：直接定位，代价与页码无关
SELECT * FROM videos
WHERE create_time < ?
   OR (create_time = ? AND id < ?)
ORDER BY create_time DESC, id DESC
LIMIT 10;
```

配合 `idx_videos_create_time` 索引，第 1 页和第 10000 页的代价几乎一样。

**更隐蔽的问题是翻页错乱**：用户看第 1 页时，有人发布了 3 条新视频。翻第 2 页时：

- OFFSET：整体后移 3 位，第 2 页开头会**重复**第 1 页末尾的 3 条
- 游标：新数据插在游标之前，**不参与后续分页**，不重复也不漏

**为什么必须是复合游标**：只用 `create_time`，同一毫秒有多条视频时，`create_time < ?` 会把同毫秒的其余条目全部跳过——漏数据。热榜早就用了 `(popularity, id)`，时间线现在也统一到 `(create_time, id)`：

- 冷区：`WHERE create_time < ? OR (create_time = ? AND id < ?)`，严格按 `(create_time DESC, id DESC)` 排序，与热区、索引定义三处一致；
- 热区：不能直接对 ZSET 做「字典序小于某 id」的范围查询，所以改成 **`maxScore = reqTime`（包含等号）+ 应用层过滤**——先用 `ZCOUNT key reqTime reqTime` 算出同毫秒条数，一次性取回，再在内存里过滤掉 `score == reqTime && id >= cursor.ID` 的成员，最后按 `(score DESC, id DESC)` 排序截断。

**代价**：同毫秒的条数不可控，`ZCOUNT` 存在的意义就是把「要拉多少条」提前算出来，避免少取导致漏项。正常发布速率下同毫秒只有个位数，代价可以忽略；极端刷量场景下这一页会变重，但不会错。

---

### Q12：冷热边界怎么缝合？为什么冷数据不回填缓存？

**要点**：热区取不满时，用热区最后一条的 `(CreateTime, ID)` 当冷游标去 MySQL 补齐；补齐结果**不写回 ZSET**；冷区查询失败要向上报错，不能静默返回短页。

**完整回答**：
跨边界的那一页会走三步：

1. 从 Redis 拿热区 ID 列表（`ZREVRANGEBYSCORE`），目标是 `want := limit + 1` 条——多取的一条只用于探测 `has_more`；
2. 如果不足 `want` 条，说明这一页跨过了冷热边界，用**热区最后一条的复合游标** `(CreateTime, ID)` 去 MySQL 补 `want - len(baseVideos)` 条；
3. 拼接后判断 `hasMore := len(baseVideos) > limit`，截断到 `limit` 返回；游标取**实际返回的最后一条**，而不是多取的那条。

关键点在**不回填**：如果不小心把补齐的冷数据写回 ZSET，那么任何用户多翻几页，就能把大量历史数据灌进只保留 1000 条的热池，**把真正的新视频挤出去**——这就是「冷数据污染热点池」。一次误操作就能让热区的命中率归零。

**只缝一次就够了**：冷区是 MySQL 全量数据，热区是它的子集，且冷区查询没有任何后置过滤（不像关注流还要按关注关系筛）。所以跨过边界后，剩余部分一定全部命中冷区，不存在「冷区再跨回热区」的可能。反过来，如果冷区查询失败，正确做法是**把错误抛给调用方**，让客户端重试——静默返回一个短页，用户会以为没数据了，这种「看起来成功」的降级比报错更有害。

**加分点**：判断走热区还是冷区的依据，是**请求游标与 ZSET 最老一条 score 的比较**：

```go
if reqTime <= watermark { /* 冷区：查 MySQL */ } else { /* 热区：查 Redis */ }
```

这个 `watermark` 是动态计算出来的，不需要维护额外的元数据——ZSET 本身的状态就是冷热边界。之所以用 `<=` 而不是 `<`：裁剪是按 rank 切的，同一毫秒的成员可能被切成两半，剩下那半条 score 正好等于水位线，必须去 MySQL 才能把同毫秒的其余条目补齐。

---

### Q13：Redis 全挂了 Feed 还能用吗？ZSET 空了怎么重建？

**要点**：能。`rediscache == nil` 或 ZSET 查询报错都直接降级 `listLatestFromDB`；ZSET 为空时用 singleflight 从 MySQL 拉最新 1000 条重建；只丢了一部分时靠 5 分钟对账补齐。

**完整回答**：
四层自愈，按「丢失程度」递进：

1. **Redis 整体不可用**：`ListLatest` 开头判断 `f.rediscache == nil`，直接走 `listLatestFromDB`，纯 MySQL 游标查询，服务可用（慢一些）。
2. **Redis 超时/报错**：取 `zsetTail` 出错也降级走 DB，不会把错误抛给用户。
3. **ZSET 是空的**（冷启动 / Redis 被清）：触发重建——用 singleflight 保证只有一个请求去重建，从 MySQL 拉最新 `TimelineCapacity` 条 `ZADD` 回 ZSET，其余请求等待；如果 DB 也是空的，返回 `EMPTY_DB` 哨兵**防无限递归**。
4. **ZSET 非空但少了几条**（消费端进了死信、Lua 执行中断、运维误删）：空判定不会触发，靠**每 5 分钟一次的对账**——读 MySQL 最新 1000 条与 ZSET 成员做差集，**只补缺失的，不按差集删除多出来的**（补完再按容量裁剪一次，维持 1000 条上限）。

**删除的对称处理**：视频被删除时，`Delete` 除了删 DB 行、失效实体缓存，还会尽力 `ZRem` 热时间线（100ms 超时）。Redis 失败不回滚删除——DB 才是源数据，读路径会过滤查不到的 ID，5 分钟对账也不会把它加回来（MySQL 里已经没有这条了）。残留成员不会被对账主动删除（对账只补不删），最终靠后续写入的 rank 裁剪自然挤出，这段时间只是浪费几个热区名额，不影响正确性。

**加分点**：对账为什么「只补不删」？因为对账用的是 MySQL 的**某个时刻快照**，而对账执行期间可能刚有新视频发布进 ZSET、还没进快照。此时「ZSET 有、快照没有」不代表多余，删掉就是误伤。补是安全的（幂等 `ZADD`），删是不安全的——**自愈逻辑要优先选择幂等且可重入的方向**。

另外要能说清对账的**检测窗口**：它每 5 分钟跑一次，意味着部分丢失最多 5 分钟不可见。这是有意的取舍——为几条派生数据做实时校验，成本远高于收益；而源数据本身有 Outbox 保证，不会丢。

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
1. **控制存量**：已经发送的消息不需要永久保留。`PurgeSent` 每 10 分钟清理 24 小时前的 `sent` 记录，按 1000 条/批循环删除，稳态下表里只有「未投递 + 近一天已投递」，行数与**投递速率**相关而不是与**历史总量**相关。
2. **保证查询不劣化**：热查询是 `WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at <= ?) ORDER BY id LIMIT 100`，需要 `(status, next_retry_at, id)` 的复合索引；`CountPending` 走 `status` 索引。只要行数在索引可控范围内，查询代价与总量无关。
3. **归档**：`failed` 是终态，长期堆积说明有系统性问题。现在有 `failed > 0` 告警 + `/admin/outbox/failed` 查询 + `/replay` 重放；但**历史归档表还没做**，长期存量仍要靠人工重放/清理。
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

**要点**：Outbox 的状态在数据库里，一条条件聚合 SQL 就能拿到全部核心指标；本项目已把统计、`/metrics` gauge、`[ALERT]` 日志和管理接口全部落地。

**完整回答**：
`OutboxRepository.Stats` 用一条 SQL 同时算出 pending / processing / failed / stuck / 最老 pending / 最大重试次数，poller 每分钟调用一次：

| 指标 | 来源 | 告警阈值 |
|---|---|---|
| 积压量 | `SUM(status='pending')` | 持续增长超过 N 分钟 → poller 或 MQ 有问题 |
| 卡死量 | `SUM(status='processing' AND claimed_at < now-60s)` | > 0 → 立刻 `[ALERT]` |
| 失败量 | `SUM(status='failed')` | > 0 → 立刻 `[ALERT]`，需人工介入 |
| 最老积压时长 | `MIN(create_time) WHERE status='pending'` | 超过 5 分钟 → `[ALERT]`，投递链路可能断了 |
| 峰值重试次数 | `MAX(retry_count)` | 接近 5 → 即将转 failed |

三路输出：

1. **日志**：异常时打 `[ALERT]` 前缀，日志采集可直接按关键字告警；
2. **指标**：`feedsystem_outbox_messages{status="pending|processing|failed|stuck"}`、`feedsystem_outbox_oldest_pending_seconds` 写入内部观测端口的 `/metrics`（与 pprof 共用，不暴露在业务端口）；
3. **接口**：`GET /admin/outbox/stats`、`GET /admin/outbox/failed`、`POST /admin/outbox/replay`（`X-Admin-Token` 保护，仅在配置 `ADMIN_TOKEN` 时注册）。

消费端通过统一 `Consumer` 骨架打点：`feedsystem_consumer_processed_total{result}`、`feedsystem_consumer_retry_total`、`feedsystem_consumer_deadletter_total`、`feedsystem_dlq_consumed_total`。

**加分点**：把「失败」显式建模成 `failed` 终态而不是「重试几次然后静默丢弃」，是为了**让问题可见**。更进一步是让它**可干预**——能查、能重放，才算闭环。

---

### Q19：这个模块还有哪些已知不足？

**要点**：诚实列出，并给出改进方向。

**完整回答**（主动暴露问题比被追问出来好得多）：

**已解决（第三轮，可运维性）**：

- `failed` 有周期统计、`[ALERT]`、`/admin/outbox/failed` 查询和 `/admin/outbox/replay` 重放；
- 发布端退避已加 ±30% 抖动；
- confirm 已改成 `DeferredConfirmation` 按 `DeliveryTag` 精确匹配，不再依赖串行前提；
- DLX 绑定已改成精确路由键；DLQ 消费者会落 `dead_letters` 表并打指标。

**已解决（第四轮，冷热分离查询）**：

- 写入 + 裁剪合并成一段 Lua，`TimelineCapacity` 三处共用，不再出现「重建完立刻被裁」；
- 时间线游标统一成 `(create_time, id)` 复合游标，同毫秒不再漏项；热区用 `maxScore = reqTime` + `ZCOUNT` 同分条数 + 应用层按 id 过滤；
- `has_more` 改为「多取一条」独立探测，跨冷热边界那页不再误报；
- 缝合用复合冷游标、`want - len` 精确补数，冷区查询失败直接报错而不是静默返回短页；
- 删除视频时尽力 `ZRem` 热时间线；关注流从「整页结果缓存 24h」改为**只缓存 ID 页 + 游标**，内容命中后重新装配，TTL **首页 30s、后续页 24h**（新视频只影响首页）；
- 新增每 5 分钟对账，补齐 ZSET 部分丢失（只补不删）。

**仍然存在的边界**：

1. **消费端仍有一次进程内阻塞重试**：默认骨架 1+3 次；时间线已降为 `MaxLocalRetries=1`，但这一条失败消息仍会短暂阻塞 QoS=10 的流。更彻底的做法是本地 0 次、直接进 TTL 重试队列。
2. **回收不区分「崩溃」与「慢」**：60s 租约一刀切，慢实例会被误回收导致重复投递。生产应做租约续期（处理中定期刷新 `claimed_at`）。
3. **DLQ 没有自动重放工具**：消息已落表、可按 `event_id` 查询，但重投仍要人工把消息体重新发布回主交换机；自动重放有毒消息只会形成「进死信 → 重放 → 再进死信」的循环。
4. **`failed` 没有历史归档**：长期失败的存量仍要靠人工重放/清理；千万级时还需要按时间分区。
5. **永久性拓扑损坏只会告警，不会终态化**：交换机/队列被误删时消息会一直 `pending`，由「最老 pending > 5min」的 `[ALERT]` 发现，而不是自动转 `failed`。
6. **`mandatory=false` 的不可路由消息会被 broker ACK**：本地会标 `sent`。要堵住需要 `mandatory=true + NotifyReturn`。
7. **轮询仍是固定 1s**：自适应轮询、Redis 信号、CDC 都还在路线图里，没有实现。
8. **部分丢失有 5 分钟检测窗口**：对账不是实时校验，这段时间内热区会缺几条，用户翻到冷区可见。为派生数据做实时校验不划算。
9. **冷区 `OR` 条件要靠 `EXPLAIN` 兜底**：`create_time < ? OR (create_time = ? AND id < ?)` 在 MySQL 8 上通常能走 range + index merge，但这依赖优化器；上线前必须 `EXPLAIN ANALYZE` 确认没有退化成分组扫描 + filesort。
10. **热区同毫秒条数没有上界**：`ZCOUNT` 会把整毫秒的成员一次性拉回内存做 id 过滤。正常发布速率下是个位数，但刷量场景下这一页会明显变重；要彻底解决得改 score 编码（如 `create_time * 1000 + seq`）或改用复合 member。
11. **容量是静态的、且只有一个全局 ZSET**：1000 条按发布速率换算成时间窗口会浮动，高发布量下热区可能只覆盖几分钟；也不支持按标签/关注流分片。真要做需要动态容量 + 多 ZSET。
12. **关注流仍是读扩散**：`author_id IN (子查询)` 在关注数大时扫描量不可控。取关会主动 `DelByPattern` 清掉该账号的 ID 页缓存，正常路径立即生效；剩余窗口是「并发回填竞态」——一个已经在查库的请求可能把旧 ID 页写回，按页 TTL 收敛（首页 30s、深层页 24h）。写扩散（推模式）是另一个量级的改造，本项目没做。

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
| **B3. confirm 流水线** | 连发 N 条再统一收 N 个 ACK | 把 N×RTT 压成 1×RTT | 单条精确等待已用 `DeferredConfirmation` 实现；要进一步流水线化，需要改成「先发 N 条、再按 `DeliveryTag` 收集 N 个结果」 |
| **C. 查询优化** | 用 `visible_at` 哨兵值（默认 0）替掉 `OR ... IS NULL`，建 `(status, visible_at, id)` 复合索引 | 从逐行过滤变索引范围扫描，积压越重收益越大 | 字段语义微调 |
| **D. id 水位线** | 记 checkpoint，`id > lastMaxID` 增量拉 | 不重复扫历史 | 需拆两条路：增量流 + 重试到期流 |
| **E. Redis 信号** | 业务提交后 `LPUSH outbox_signal`；poller 用 `BRPOP` 阻塞，有信号才查库 | DB 压力≈0，毫秒级延迟 | 信号会丢，必须有兜底慢轮询（30s 全量扫） |
| **F. CDC** | Canal/Debezium 订阅 binlog 推送 | 毫秒级、空闲零开销 | 新组件 + 位点 + 高可用 + 排障 |

**✗ 不推荐的**：为 Outbox 分库分表。表靠 `PurgeSent` 一直很小，行数与**投递速率**相关而非历史总量，分表解决的是错误的问题。

**本项目路线图**：

| 阶段 | 动作 | 触发条件 |
|---|---|---|
| 已完成 | B3 的精确 confirm（`DeferredConfirmation` + `DeliveryTag`） | — |
| 现在 | A（自适应轮询）+ C（`visible_at` 哨兵） | 无条件，成本极低 |
| 发布量上来 | B1 + B2（事务保持短） | 单批处理成瓶颈 |
| 再往后 | B3 流水线化（批量确认） | 单条 RTT 成瓶颈 |
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
   ↓ video_service.go:62    写进 OutboxMsg（与 event_id 同事务）
OutboxMsg.CreateTime
   ↓ outboxworker.go:140    tmq.PublishVideo(pubCtx, msg.VideoID, msg.CreateTime, eventID)
TimelineEvent.CreateTime
   ↓ timelineMQ.go:111      CreateTime: createTime.UnixMilli()   ← 转毫秒
   ↓ outboxworker.go:317-318    Lua: ZADD score=float64(event.CreateTime)
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
2. **游标被"吃掉"**：游标分页虽然用了 `maxScore = reqTime`（含等号）+ 应用层按 id 过滤，但同分的成员必须先全部取回才能过滤。秒级精度下同一秒可能有成百上千条，`ZCOUNT` 会把整秒的成员拉进内存，代价不可控。

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
| **排序取最新** | `ZREVRANGEBYSCORE key maxScore -inf LIMIT 0 count` |
| **裁剪 1000 条** | Lua 内 `ZREMRANGEBYRANK key 0 -(capacity+1)`（与 `ZADD` 原子执行） |
| **冷热水位线** | `watermark := int64(zsetTail[0].Score)` |
| **同分探测** | `ZCOUNT key reqTime reqTime`，决定热区一页要拉多少条 |
| **游标分页** | `maxScore = reqTime`（含等号）+ 应用层过滤 `score == reqTime && id >= cursor.ID` |

注意：**热区是「最近 1000 条」而不是「最近 N 小时」**，容量按条数封顶，所以 `watermark` 是随发布速率浮动的时间点——发布越频繁，热区覆盖的时间窗口越短。另外 score 只存了毫秒，`id` 并没有编码进 score，所以同毫秒的排序只能靠应用层补，这也是为什么热区分页不能做成纯 Redis 范围查询。

---

## 13. 本次改动清单

| # | 文件 | 改动 |
|---|---|---|
| 1 | `rabbitmq/timelineMQ.go` | 开启 publisher confirm + `waitConfirm`（第三轮已替换为 `PublishWithDeferredConfirm`）+ `Close()` + `DeclareTimelineTopic()` |
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

### 第三轮改动（可运维性 + 故障分类）

| # | 文件 | 改动 |
|---|---|---|
| 14 | `video/video_entity.go` | `OutboxBackoff` 加 ±30% 抖动；新增 `OutboxBrokerRetryDelay = 5s`；`OutboxMsg` 新增 `LastError` |
| 15 | `video/outbox_repo.go` | 新增 `MarkDeferred`（不烧额度）、`Stats`、`ListFailed`、`ReplayFailed`；`MarkRetry/MarkFailed/ReapStuck` 写 `last_error`；`PurgeSent` 改 1000 条/批循环 |
| 16 | `rabbitmq/timelineMQ.go` | 改用 `PublishWithDeferredConfirm`；新增 `ErrBrokerUnavailable` / `ErrPublishRejected` 错误分类 |
| 17 | `worker/outboxworker.go` | broker 级故障走 `MarkDeferred` 并重建 channel；每分钟 `Stats` + `[ALERT]`；时间线消费者 `MaxLocalRetries=1`；新增 `errText` |
| 18 | `worker/retry.go` | `Consumer` 支持 `MaxLocalRetries`，不再写死 3 |
| 19 | `http/outbox_admin.go` + `http/router.go` | 新增 `/admin/outbox/stats|failed|replay`，`X-Admin-Token` 常量时间校验 |
| 20 | `config/loadconfig.go` + `cmd/main.go` | 新增 `ADMIN_TOKEN` 配置/环境变量并注入 router |
| 21 | `worker/deadletter.go` + `worker/dlqworker.go` | DLQ 消费者把死信落 `dead_letters` 表（`(queue,event_id)` 唯一 + `INSERT IGNORE`），失败重试 3 次后 Ack |
| 22 | `observability/metrics.go` | `/metrics` 输出 `feedsystem_outbox_*` gauge 与消费端计数 |

> `last_error`、`dead_letters`、`ADMIN_TOKEN` 都随启动流程自动生效；`last_error` 由 `AutoMigrate` 自动加列。

### 第四轮改动（冷热分离查询修复）

| # | 文件 | 改动 |
|---|---|---|
| 23 | `video/timeline.go`（新增） | 抽出 `TimelineCapacity = 1000` 与 `TimelineZSetKey`，写入、重建、对账三处共用 |
| 24 | `worker/outboxworker.go` | `zaddTimeline` 改成一段 Lua：`ZADD` + `ZREMRANGEBYRANK 0 -(capacity+1)` + `ZCARD` 原子执行 |
| 25 | `feed/entity.go` | 新增 `TimelineCursor{CreateTime, ID}`；响应新增 `has_more` |
| 26 | `feed/repo.go` | `ListLatest` 改复合游标 `(create_time < ?) OR (create_time = ? AND id < ?)`，排序 `(create_time DESC, id DESC)` |
| 27 | `feed/service.go` | `ListLatest` 改为 `want = limit+1` 探测 `has_more`；热区 `maxScore = reqTime` + `ZCOUNT` 同分条数 + 应用层 id 过滤；缝合用复合冷游标且冷区失败向上报错；新增 5 分钟对账 `StartTimelineReconciler`；关注流缓存只存 ID、首页 TTL 30s、`has_more` 用 `limit+1` |
| 28 | `feed/handler.go` | 校验 `latest_time` / `latest_id_before` 必须成对出现，limit 做上限钳制 |
| 29 | `video/video_service.go` | 删除视频后尽力 `ZRem` 热时间线（100ms 超时，失败不回滚） |
| 30 | `middleware/redis/zset.go` | 新增 `ZRem`，移除重复的 `Exists` 声明 |
| 31 | `http/router.go` | 启动时拉起 `StartTimelineReconciler`，周期 `5 * time.Minute` |

> 复合游标上线后，旧客户端只传 `latest_time` 会被直接 400 拒绝（handler 强制两个字段成对且非零），不存在「悄悄退化成严格小于」的灰区——宁可让客户端立刻发现参数错了，也不要在同毫秒数据上静默漏项。对账任务只在 API 进程内跑，多实例会重复执行，但因为「只补不删 + `ZADD` 幂等」，重复执行无副作用。

---

## 14. 还没做的（下一步）

按优先级（与 Q19 一致）：

1. **自适应轮询 + `visible_at` 哨兵**：空闲期把 DB 查询降到 1/5，并让 `ListDue` 稳定走索引范围扫描。零成本纯收益，优先级最高。
2. **消费端本地重试降到 0**：时间线已经是 1+1，但这一条仍会短暂阻塞 QoS=10 的流；更进一步是本地不 sleep，直接进 TTL 重试队列。
3. **租约续期**：60s 租约仍不区分「崩溃」与「慢」，慢实例会被误回收；生产应定期刷新 `claimed_at`。
4. **DLQ 重放工具**：消息已落 `dead_letters`、可按 `event_id` 查询，但重投仍需人工。要做成工具的话必须带审计日志和限速，且**不要自动重放**——有毒消息会无限循环。
5. **`failed` 历史归档 / 分区**：目前靠告警 + 人工重放控制存量；千万级时应归档到历史表或按 `create_time` 分区。
6. **`mandatory=true + NotifyReturn`**：堵住 `mandatory=false` 下不可路由消息被 broker ACK、本地却标 `sent` 的洞。
7. **永久性拓扑损坏的自动判定**：目前只会持续 `MarkDeferred` 并靠「最老 pending」告警；可增加「连续 N 次 broker 级失败且 pending 不降」的独立告警/熔断策略。
8. **冷区执行计划要 `EXPLAIN` 兜底**：`create_time < ? OR (create_time = ? AND id < ?)` 依赖优化器选 range + index merge，上线前需 `EXPLAIN ANALYZE` 确认没有退化成分组扫描 + filesort；必要时改写成 `(create_time, id) < (?, ?)` 的元组比较。
9. **热区同毫秒条数无上界**：`ZCOUNT` + 应用层过滤意味着刷量场景下一页会明显变重。彻底解法是把 id 编码进 score（如 `create_time * 1000 + seq`，需重新评估 float64 精度）或改成复合 member。
10. **容量静态、只有单个全局 ZSET**：1000 条换算成时间窗口随发布速率浮动，也不支持按标签/关注流分片；要动态容量或多 ZSET 需要额外的元数据与裁剪策略。
11. **关注流仍是读扩散 + 缓存成员**：`author_id IN (子查询)` 在关注数大时扫描量不可控；取关会主动清掉该账号的 ID 页缓存，但并发回填竞态仍可能把旧成员写回，按页 TTL 收敛（首页 30s、深层页 24h）。写扩散是另一个量级的改造，本项目没做。
