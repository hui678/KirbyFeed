# Outbox 面试问题汇总（19 问）

> 题源：`chatgpt的面试问题设计.md` 第三节「Outbox 可靠投递」
> 对应实现：`backend/internal/video/`、`backend/internal/worker/outboxworker.go`、`backend/internal/middleware/rabbitmq/`
> 相关学习文档：`冷热分离时间线与Outbox.md`（本文在其基础上补全 19 问的完整口径）

---

## 0. 先背这条链路（30 秒总述）

被问「讲讲你的 Outbox」时，先给这一条，再按追问展开：

```
MySQL 事务（videos + outbox_msgs 同一 tx）
   → poller 每秒 ListDue 100 条
   → Claim：CAS pending→processing + fencing token
   → Publish：persistent 消息 + publisher confirm（5s）
   → MarkSent / MarkRetry（指数退避）/ MarkFailed
   → RabbitMQ 主队列（手动 ACK，QoS=10）
   → Consumer 三级容错（进程内退避 → 重试队列 TTL → 死信队列）
   → ZADD feed:global_timeline（member=video_id，score=create_time 毫秒）
   → 冷热分离查询
```

**三句话内核：**

1. 用**单库事务**把「跨系统原子性」降维成「本地消息表 + 后台重投」；
2. 用**幂等**把「不重复投递」降级成「重复了也无害」，得到的是 **at-least-once 投递 + exactly-once 效果**；
3. 用**显式状态机**（pending/processing/sent/failed）把失败变成可观测、可干预的状态，而不是静默丢弃。

**关键参数（被问数字时必须张口就来）：**

| 参数 | 值 | 位置 |
|---|---|---|
| 轮询间隔 / 单批条数 | 1s / 100 条 | `worker/outboxworker.go:19-20` |
| 抢占租约（processing 超时） | 60s | `video/video_entity.go:62` |
| 回收扫描间隔 / 单批上限 | 30s / 1000 条 | `worker/outboxworker.go:21`、`video/outbox_repo.go:14` |
| 最大重试次数 | 5 次（首次 + 5 次重试 = 6 次投递尝试） | `video/video_entity.go:58` |
| 退避 | 2s × 2^(n-1)：2/4/8/16/32s，累计约 62s | `video/video_entity.go:60,68` |
| publisher confirm 超时 | 5s | `middleware/rabbitmq/timelineMQ.go:30` |
| 单条发布 ctx 超时 | 10s | `worker/outboxworker.go:123` |
| sent 保留 / 清理间隔 | 24h / 10min | `video/video_entity.go:64`、`worker/outboxworker.go:22` |
| 消费端本地重试 / 重试队列轮数 | 1+3 次 / 3 轮，TTL 30s | `worker/retry.go:18`、`middleware/rabbitmq/retry.go:17` |

**代码地图：**

| 文件 | 职责 |
|---|---|
| `video/video_service.go:48-83` | 业务数据 + Outbox 同事务写入，事务内生成 `event_id` |
| `video/video_entity.go` | 表结构、状态常量、退避函数 |
| `video/outbox_repo.go` | ListDue / Claim / MarkSent / MarkRetry / MarkFailed / ReapStuck / PurgeSent |
| `worker/outboxworker.go` | poller（投递）+ timeline consumer（写 ZSET） |
| `middleware/rabbitmq/timelineMQ.go` | 发布封装 + publisher confirm |
| `worker/retry.go` | 消费端统一容错骨架（三级重试） |

---

# 第一层：基础原理

## 1. 什么是 Outbox 模式？它解决了什么问题？

**一句话：** 把「要发的消息」当成一行业务数据，和业务数据写进**同一个数据库事务**；事务提交即代表消息「一定存在」，再由后台 poller 把它读出来发出去。

**展开：**

发布视频要同时做两件事：写 `videos` 表、发一条「有新视频」的 MQ 消息。这两件事跨了 MySQL 和 RabbitMQ 两个系统，**没有共同的事务边界**。Outbox 的做法是：

```sql
BEGIN;
  INSERT INTO videos (...);
  INSERT INTO outbox_msgs (video_id, event_type, status, event_id, ...);
COMMIT;
```

这样就把「跨系统原子性」问题，转换成了「单库事务 + 后台可重试投递」问题。它保证三件事：

1. **同生共死**：业务数据回滚，消息记录也回滚；业务数据提交，消息记录一定在（进程随后崩溃也不影响）；
2. **可重试**：投递失败不是「报错给用户」，而是留在表里按退避策略继续投；
3. **可观测**：积压量就是一条 `SELECT COUNT(*)`，比查 MQ 内部状态容易得多。

**它不是「消息不丢」的完整答案**，只解决了「发布端不会因为发送失败/崩溃而永久丢掉这次业务事件」。端到端还依赖：publisher confirm（确认 broker 收下）、手动 ACK（确认消费成功）、消费端幂等（重复无害）、死信与告警（失败可见）。

**边界：** Outbox 用「延迟」换「原子性」。本项目轮询 1s，所以发布后约 1s 才进热时间线。这是明确的取舍，不是缺陷。

**加分：** Outbox 最被低估的优势是可观测性——`CountPending` 一条 SQL 就能做积压告警；而 MQ 里的堆积通常要进管理台或调 API 才能看清。

---

## 2. 为什么不能在 MySQL 事务提交后直接发送 RabbitMQ 消息？

**一句话：** 提交和发送之间没有原子性，任何顺序都有丢消息或脏消息的窗口，而且失败后**没有任何持久化记录**可以补。

**展开（按四种写法拆）：**

| 写法 | 失败窗口 | 后果 |
|---|---|---|
| 先写库、提交后再发 MQ | 提交成功，发送失败 / 进程崩溃 / 网络断 | 视频存在，但热时间线永远没有它；进程内重试也随崩溃一起消失 |
| 先发 MQ、再写库并提交 | 消息已发出，事务回滚 | 消费端拿到一个不存在的 `video_id`（可能因隔离级别读不到未提交行），时间线指向脏数据 |
| 发送成功、但应用以为失败并重试 | 网络抖动、ACK 丢失 | 重复消息 |
| 提交与发送之间进程崩溃 | 状态未知 | 无法判断该补哪一边 |

关键点：**「待发送」这件事只存在于进程内存里**，进程一死就没了。Outbox 的价值就是给它一个持久化的落点。

还要注意：即使你在进程内加 `for retry`，也解决不了「提交成功后进程立刻崩溃」——重试代码根本没机会执行。

**加分：** 这题的本质是「两个系统之间没有分布式事务」。Outbox 不是让双写变原子，而是**把第二个写操作也变成数据库写**，从而只用一个本地事务。

---

## 3. 为什么不采用分布式事务、两阶段提交，或者单纯依靠 Publisher Confirm？

**一句话：** 2PC/TCC 太重且 MQ 通常不是 XA 资源；Publisher Confirm 只回答「broker 收没收到」，不回答「业务事务提没提交」。

**逐个对比：**

| 方案 | 能解决什么 | 为什么不用 |
|---|---|---|
| **2PC / XA** | 跨库原子提交 | RabbitMQ 不是标准 XA 资源；需要协调者、同步阻塞、故障时资源悬挂；为一条时间线消息引入全局事务，运维成本远超收益 |
| **TCC / Saga** | 业务级最终一致 | 要为「发消息」设计 Try/Confirm/Cancel 和空回滚、悬挂处理，补偿逻辑的 bug 面比 Outbox 大得多 |
| **只靠 Publisher Confirm** | 发布端知道 broker 是否收下 | 它是**面向 MQ 的**确认，不覆盖「业务写库」这一段。先发后提交：confirm 成功但事务回滚 → 脏消息；先提交后发：confirm 失败仍丢消息 |
| **RocketMQ 事务消息** | 半消息 + 事务回查 | 方案成立，但强绑定具体 MQ 产品，还要实现本地事务状态回查接口；本项目用 RabbitMQ |
| **Outbox** | 单库事务 + 可重试投递 | 代价是延迟（约 1s）和轮询压力，但实现简单、可观测、与 MQ 选型解耦 |

**关键辨析：** Publisher Confirm 不是 Outbox 的替代品，而是 Outbox 的**必要组件**——Outbox 负责「消息不会丢记录」，Confirm 负责「记录可以安全地置为 sent」。

**加分：** 如果业务能接受「消息可能重复、但绝不能丢」，Outbox + confirm + 幂等是最小充分方案；如果连 1s 延迟都不能接受，再考虑 CDC（Canal/Debezium）订阅 binlog，而不是上 2PC。

---

## 4. Outbox 表应该有哪些字段？分别有什么作用？

**一句话：** 至少要有「业务引用 + 事件类型 + 状态 + 重试元数据 + 幂等键 + 抢占令牌」，本项目还刻意**不存 payload**。

**本项目的真实结构（`video/video_entity.go:79-105`）：**

| 字段 | 作用 | 设计要点 |
|---|---|---|
| `id` | 主键、扫描顺序、抢占单元 | `ListDue` 按 `id ASC` 取，天然近似 FIFO |
| `video_id` | 业务引用 | 消费端只需要它写 ZSET |
| `event_type` | 事件类型 | 未来一张表承载多种事件时用于路由/过滤 |
| `create_time` | 事件发生时间 | 也是重投时消息体里的 `create_time` 来源，保证 score 不漂移 |
| `status` | 状态机 | `pending / processing / sent / failed`，有索引 |
| `retry_count` | 已重试次数 | 决定退避时长，也是转 `failed` 的依据 |
| `next_retry_at` | 下次可见时间 | 退避期间不可见，避免故障时忙轮询；有索引 |
| `claimed_at` | 租约时间戳 | 判断 `processing` 是否超时、能否被回收 |
| `sent_at` | 确认时间 | `PurgeSent` 按它清理 24 小时前的记录 |
| `event_id` | **稳定幂等键** | 唯一索引 `idx_outbox_event_id`；在写 outbox 的同一事务里生成，重投不换 ID |
| `claim_token` | **fencing 令牌** | 每次抢占随机生成；所有状态迁移都带 `WHERE claim_token=?`，防止旧持有者误改新持有者的状态 |

**为什么没有 `payload` 列？** 这条消息的事件体是「引用型」的：`{event_id, video_id, create_time}`，由 poller 在发送时用行里的字段现场构造（`outboxworker.go:117-124`）。消费端也只需要 `video_id + create_time` 就能写 ZSET。详见第 17 问。

**通用化时怎么加：** 如果事件体复杂（比如通知文案、审计快照），就加 `payload JSON`；如果事件需要按聚合根排序，再加 `aggregate_id / version`。

**索引设计：**

- `status` 单列索引（`CountPending`、按状态清理）；
- `next_retry_at` 索引；
- 更优的是复合索引 `(status, next_retry_at, id)`，让 `ListDue` 走索引范围扫描；
- `event_id` 唯一索引——注意历史行用 `NULL` 而不是 `''`，否则 `AutoMigrate` 建唯一索引会立刻冲突（MySQL 唯一索引允许多个 NULL）。

**追问「为什么用指针类型」：** `EventID *string`、`ClaimToken *string` 是为了表达「可空且唯一索引可容纳多个 NULL」，这是迁移老表时的实际约束，不是风格问题。

---

## 5. RabbitMQ 的 Publisher Confirm 和 Consumer ACK 分别解决什么问题？两者有什么区别？

**一句话：** Confirm 管「发布端 → broker」，ACK 管「broker → 消费端」，两段独立、都只覆盖自己那一段，合起来也不等于端到端成功。

**对比表：**

| | Publisher Confirm | Consumer ACK |
|---|---|---|
| 方向 | Producer → Broker | Broker → Consumer |
| 回答的问题 | broker 是否收下并（对持久化消息）落盘 | 消费者是否处理成功、能否删除这条投递 |
| 触发时机 | `Publish` 写入 socket 之后异步回执 | 业务处理完成后手动 Ack |
| 失败表现 | NACK / 超时 / 通道关闭 | Nack(requeue) 重投 / Nack(false) 进死信 |
| 本项目位置 | `timelineMQ.go:57-66,116-137` | `worker/retry.go:46`（`autoAck=false`）+ `handle` |

**必须讲清的两个细节：**

1. **`Publish` 返回 nil 只代表写进了本地 socket**，不代表 broker 收到。所以 `PublishJSON` 成功之后还要等 `waitConfirm`，收到 `Ack=true` 才能 `MarkSent`。
2. **Confirm ACK 不等于「路由成功」**：如果消息没有匹配到任何队列（绑定缺失），在 `mandatory=false` 时 broker 会丢弃它但仍然回 ACK。所以发布端必须确保拓扑已声明（本项目 `DeclareTimelineTopic` 在 `NewTimelineMQ` 里做），需要更强保证时加 `mandatory=true` + `NotifyReturn`。

**持久化的三个条件：** durable exchange + durable queue + `DeliveryMode=Persistent`。本项目三者都满足（`rabbitMQ.go:117-122`）。缺一个，broker 重启后消息仍可能丢。

**为什么两者不能互相替代：**

- 只有 confirm：消息安全到了 broker，但消费者可能处理失败后静默 ACK 丢掉；
- 只有 consumer ACK：消费端很可靠，但发布端可能以为发出去了、实际 broker 没收到。

**加分：** 「Confirm 是发布端语义，ACK 是消费端语义；端到端可靠 = confirm + 持久化 + 手动 ACK + 幂等 + 重试/死信」。这句话能把整条链路串起来。

---

# 第二层：实现细节

## 6. 业务数据和 Outbox 消息如何保证在同一个 MySQL 事务中提交？请写出 Go 伪代码。

**一句话：** 用同一个 `*gorm.DB` 事务对象连续写两张表，任何一步失败整体回滚；**绝不在事务里发 MQ**。

**本项目的真实代码（`video/video_service.go:48-83`）：**

```go
err := vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
    if err := tx.Create(video).Error; err != nil {
        return err
    }

    eventID, err := rabbitmq.NewEventID()   // 幂等键在事务内生成
    if err != nil {
        return err
    }
    msg := OutboxMsg{
        VideoID:    video.ID,
        EventType:  "video_published",
        Status:     OutboxStatusPending,
        CreateTime: video.CreateTime,
        EventID:    &eventID,
    }
    if err := tx.Create(&msg).Error; err != nil {
        return err
    }

    // 标签也在这个事务里
    // ...
    return nil
})
```

**通用伪代码：**

```
tx := db.Begin()
if err := tx.Create(&businessRow); err != nil { tx.Rollback(); return err }
if err := tx.Create(&OutboxMsg{...}); err != nil { tx.Rollback(); return err }
if err := tx.Commit(); err != nil { return err }   // 提交成功 ⇔ 消息记录存在
// 提交之后才由 poller 异步发送
```

**四个要点：**

1. **同一个 `tx`**：用 `db.Transaction` 闭包或显式 `Begin/Commit`，不要用 `db.Create`（会走另一条连接，等于两个独立事务）；
2. **`event_id` 在事务内生成**：这是幂等的前提（第 10 问）；
3. **事务内不做网络 I/O**：不调 MQ、不调 Redis、不调外部接口，否则事务被网络 RTT 拉长，行锁和连接池都会被拖住；
4. **提交失败 = 两边都不存在**：`Commit` 返回错误时不能假设成功，应该把错误返回给调用方（发布接口失败），由用户重试。

**追问「事务提交后、poller 还没发，进程崩了怎么办」：** 消息已经在 `outbox_msgs` 里，属于持久化状态；重启后 poller 第一轮就会 `ListDue` 到它（并把上次残留的 `processing` 立刻回收）。

**追问「为什么不用 GORM 的 `CreateInBatches` 之类」：** 这里的原子性来自「同一个数据库事务」，跟批量 API 无关；批量只影响性能，不影响语义。

---

## 7. 多个 Worker 同时扫描待发送消息时，如何通过 CAS 抢占任务？为什么单纯的 SELECT 再 UPDATE 不够？

**一句话：** 用一条带状态谓词的 `UPDATE ... WHERE id=? AND status='pending'`，靠 `RowsAffected` 做原子的 test-and-set；没有状态谓词或忽略 `RowsAffected`，两个 Worker 都能「抢成功」。

**先看错误写法为什么错：**

```go
// 错误：先查后改，检查和执行之间没有原子性
rows := SELECT * FROM outbox_msgs WHERE status='pending' LIMIT 100
for _, m := range rows {
    Publish(m)                                  // 两个 Worker 都会执行
    UPDATE outbox_msgs SET status='sent' WHERE id=?   // 两个都成功
}
```

两个实例在同一时刻查到同一批 `pending`，各自都认为「这批归我」，于是**每条消息稳定投递 N 次**（N = 实例数）。这不是小概率竞态，是必然发生。

**正确写法（`video/outbox_repo.go:59-78`）：**

```go
tx := r.db.Model(&OutboxMsg{}).
    Where("id = ? AND status = ?", id, OutboxStatusPending).
    Updates(map[string]any{
        "status":      OutboxStatusProcessing,
        "claimed_at":  now,
        "claim_token": token,
    })
if tx.RowsAffected != 1 {
    return "", false, nil   // 被别人抢走了，跳过
}
```

**为什么这一条 UPDATE 是原子的：**

- InnoDB 对 `WHERE id=?` 命中的行加排他锁；
- 第二个事务的 UPDATE 会阻塞等待；
- 第一个事务提交后，第二个事务在**当前读**下重新判断谓词，发现 `status` 已经不是 `pending`，匹配 0 行，返回 `RowsAffected=0`。

所以 `RowsAffected == 1` 就是「抢到了」的证明。应用层不需要额外加分布式锁。

**为什么还要 `claim_token`（fencing token）？** 因为 CAS 只保证「同一时刻只有一个持有者」，不保证「持有者一直是对的」。旧持有者卡住 60s 被回收后，新持有者抢到；旧持有者恢复后如果只按 `status='processing'` 更新，就会把**新持有者还没发成功的消息**标成 `sent`——消息永久丢失。加了 token 后，旧持有者的更新会命中 0 行，只能记日志放弃（详见第 8 问）。

**为什么不用 `SELECT ... FOR UPDATE SKIP LOCKED`？**

- 经典写法要把「发 MQ」这段网络 I/O 包在事务里，事务可能持续几十到几百毫秒，行锁一直不释放，其他实例全部阻塞；
- CAS 把临界区从「发消息」缩小到「一条 UPDATE」，拿到发送权后不持锁地慢慢发。

**但要诚实：** 如果改成**批量抢占**（短事务里 `SKIP LOCKED` + 一条批量 UPDATE，事务内没有网络 I/O），`SKIP LOCKED` 反而比逐条 CAS 更优。判断标准是「临界区里有没有不可控 I/O」，不是哪种语法更高级。

---

## 8. 抢占任务后 Worker 宕机，消息一直处于处理中怎么办？如何设计租约和超时回收？

**一句话：** `claimed_at` 就是租约；60s 未落地由 `ReapStuck` 放回 `pending`，并清 token、消耗一次重试额度；旧持有者靠 fencing token 无法再改状态。

**机制（`video/outbox_repo.go:140-189`）：**

```
每 30s 扫描一次（启动后第一轮立刻扫）：
  WHERE status='processing' AND claimed_at < now()-60s
  LIMIT 1000
  ↓
  retry_count+1 > 5  → failed（终态，告警出口）
  否则                → pending，claimed_at=NULL，claim_token=NULL，next_retry_at=NULL（立即可见）
```

**三个容易漏的点（也是本项目修过的真实缺陷）：**

1. **必须清 `claim_token`**：否则旧持有者恢复后还能改状态；
2. **回收要消耗一次重试额度**：否则「抢占 → 崩溃 → 回收 → 抢占」可以无限循环，永远不进 `failed`，也没有告警出口；
3. **`lastReap` 必须初始化为零值**：如果初始化成 `time.Now()`，进程重启后要白等 30s 才第一次回收，残留消息白白多卡 30s。

**fencing token 的完整时序（被追问时按这个讲）：**

```
T0    A: Claim(id=7) → processing, token=tok1
T0    A: Publish 卡住（MQ 半死不活）
T60   B: ReapStuck → pending, token=NULL
T60   B: Claim(id=7) → processing, token=tok2
T61   A: 恢复，MarkSent(id=7, tok1)
          → WHERE status='processing' AND claim_token='tok1' → 0 行
          → A 只记日志「已被其他实例接管」，不动状态
T61   B: 投递成功，MarkSent(id=7, tok2) → 1 行 → sent
```

**没有 token 会怎样：** A 的 `MarkSent` 命中 `status='processing'`，把 B 还没发成功的消息标成 `sent`——这条消息从此再也不会被投递。这是整个模块最隐蔽的丢消息窗口。

**无法消除的权衡：** 租约无法区分「进程死了」和「进程只是慢」。慢的情况下一条消息会被投两次。本项目用两层兜：**重复投递 → 消费端幂等**；**数据安全 → fencing token**。原则是「宁可激进回收导致重复，也不能保守到让消息卡死」——卡死就是丢。

**怎么定 60s：** 正常路径是毫秒级，60s 留了两三个数量级余量。通用原则是 `租约 > P99 处理耗时 × 足够大的系数`；如果单次处理本身很慢（调外部接口），要做**租约续期**（处理中定期刷新 `claimed_at`），而不是把超时调到很大。

---

## 9. 你如何判断一条 Outbox 消息已经发送成功？收到 Confirm ACK 后更新状态，但更新数据库失败怎么办？

**一句话：** 成功的判据是 `waitConfirm` 收到 `Ack=true`；如果此时 DB 更新失败，消息可能已经进了 broker，本地行会在 60s 后被回收并**再发一次**——选择「重复」而不是「丢失」。

**判据链条：**

```
PublishWithContext 返回 nil        ← 只代表写进 socket，不代表 broker 收到
  ↓ 还不够
waitConfirm 收到 Ack=true          ← broker 已接受并（持久化消息）落盘
  ↓ 才算成功
MarkSent → RowsAffected=1          ← 状态落地
```

三者缺一不可。只看第一个就是原实现的 bug：`PublishJSON` 返回 nil 就 `Delete` 本地行，broker 重启后消息永久丢失。

**MarkSent 失败的三种情况：**

| 情况 | 状态 | 后果 | 处理 |
|---|---|---|---|
| DB 暂时不可用 | 行仍是 `processing` | 60s 后 `ReapStuck` → `pending` → 再发一遍 | 重复，消费端幂等 |
| 进程在 publish 后、MarkSent 前崩溃 | 行仍是 `processing` | 同上 | 重复，消费端幂等 |
| `MarkSent` 返回 `ok=false`（token 失效） | 已被其他实例接管 | 本次投递结果作废；新持有者会再发 | 重复，消费端幂等 |

**为什么不能反过来「先标记 sent、再发消息」？** 那样失败方向就变成「标记成功但消息没发」——永久丢失，且没有任何机制能发现。**副作用在前、记录在后**，失败时最多重复，这是可靠性设计的基本方向。

**为什么不在这里做「发送成功后删除本地行」？** 删除会让「是否发过」失去查询依据，也无法做审计、重放和积压观测。本项目用 `sent` + 24h 后 `PurgeSent`。

**加分：** 如果 DB 长时间不可用，`processing` 行会不断被回收、重发，broker 里会出现重复消息，消费端压力上升。这是「宁可重复」的代价；生产上应对 `processing` 数量和回收次数做告警。

---

## 10. RabbitMQ 已经收到消息，但 Publisher Confirm 丢失，Worker 重发会发生什么？如何防止重复副作用？

**一句话：** 会重复投递，但不会产生重复副作用——消费端把幂等下沉到存储操作（`ZADD` 同 member 覆盖），同时 `event_id` 从写库那一刻起就固定，保证将来有非幂等副作用时也有可用的去重键。

**为什么 confirm 会丢：**

- broker 已落盘，但 ACK 在网络上丢了；
- 5s `confirmTimeout` 先触发，而 ACK 稍后才到；
- channel 断开，确认随通道一起作废；
- 进程在 publish 后、`MarkSent` 前崩溃。

这四种情况下，本地都无法区分「真的没发成功」和「其实成功了只是没收到确认」——**超时不等于失败**，只能按失败处理并重发。

**本项目的防重复副作用方式：**

```go
// ZSET member 是 video_id，重复 ZADD 只是覆盖 score，不会出现第二条
ZAdd(key, redis.Z{Score: create_time_ms, Member: video_id})
```

因为 `score` 取的是**不可变的 `create_time`**，所以重复执行的最终状态与执行一次完全相同。这就是「at-least-once 投递 + 幂等消费 = exactly-once 效果」。

**必须同时满足的前提：**

1. **幂等键稳定**：`event_id` 在写 outbox 的同一事务里生成并落库（`video_service.go:56-65`），poller 重投时从行里读出来原样带上（`outboxworker.go:117-124`）。改前是在 `PublishVideo` 里每次新生成，同一条消息两次投递带不同 ID，消费端根本没法去重——这是修过的真实缺陷。
2. **幂等下沉到存储**：`ZADD` 覆盖语义天然幂等；不要先 `SETNX` 占位再写 ZSET——占位成功但写失败会让后续重试全被挡住，**去重反而制造丢数据**（`outboxworker.go:263-275` 有完整论证）。

**如果副作用不是幂等的（计数累加、发通知）怎么办？**

| 方案 | 要点 |
|---|---|
| 唯一约束 / 去重表 | `event_id` 唯一索引，插入去重记录与业务效果在**同一事务**里 |
| Lua 原子脚本 | 判断 `event_id` + 更新计数在 Redis 单脚本内完成 |
| 状态机 / 版本号 | 只接受比当前版本新的事件，旧版本直接丢弃 |

**核心区分：**

```
投递语义（delivery）：at-least-once      ← 无法更好（除非 2PC）
效果语义（effect）  ：exactly-once        ← 靠幂等实现
```

**加分：** 「exactly-once 在分布式系统里要么代价极高、要么根本不存在；工程通用解是 at-least-once + 幂等消费，把难保证的性质换成容易保证的性质。」

---

## 11. 你使用什么策略退避重试？固定间隔、指数退避还是指数退避加随机抖动？如何设置最大重试次数？

**一句话：** 发布端用**纯指数退避**（2s 起、5 次、累计约 62s 转 `failed`）；消费端是**指数退避 + 抖动**，并外加 TTL 重试队列和死信队列。发布端目前没有 jitter，这是已知缺口。

**发布端（Outbox，`video/video_entity.go:56-77`）：**

```
第 n 次重试等待 2s × 2^(n-1)：
  2s → 4s → 8s → 16s → 32s   （n=1..5）
累计约 62s；第 5 次失败后 RetryCount=5，
下一次失败判断 5+1 > 5 → failed
```

- **为什么指数**：瞬时故障（连接抖动、锁等待）几秒内自愈；持久故障（broker 宕机）下固定 1s 重试等于持续打靶，日志和 DB 都被打爆；
- **为什么封顶 5 次**：既要给 broker 恢复留时间，又不能让一条消息无限期占着 `pending`；超过后转 `failed`，让问题**可见**、可人工介入；
- **`next_retry_at` 的作用**：失败消息在退避期内对 `ListDue` 不可见，防止「每秒重试 100 条」的空转风暴。

**消费端（`worker/retry.go`）三级：**

| 级别 | 参数 | 失败后 |
|---|---|---|
| 进程内重试 | 1 + 3 次，0.5~1s / 1~2s / 2~4s（带 jitter，封顶 8s） | 投重试队列 |
| 重试队列 | TTL 30s 后自动回主队列，最多 3 轮（`MaxRetryCount=3`） | 进死信 |
| 死信队列 | `<queue>.dlx`，只告警不自动重放 | 人工处理 |

**消费端为什么要 jitter：** 一批消息同时失败（比如 Redis 瞬断），如果退避时间相同，恢复瞬间会集体重试形成尖峰，把下游再打一次。`retry.go:119-126` 用 `wait/2 + rand(wait/2)` 把重试打散。

**发布端为什么没 jitter：** 单实例串行 poller，天然不会出现「同一秒 100 条同时重试」的尖峰。但多实例部署时，各实例的退避相位会趋同，仍建议加 ±30% 抖动——这是本模块的已知待改进项之一。

**重试次数怎么定：**

- 由「可容忍的故障恢复时间」和「告警响应时间」共同决定：62s 覆盖常见 broker 抖动，超过 62s 还没好就应该有人看；
- 区分**可重试错误**（网络、超时、NACK、5xx）和**不可重试错误**（消息体非法、路由不存在）。后者重试多少次都没用，应该直接进 `failed`/DLQ，而不是烧完额度；
- 最大次数是配置常量（`OutboxMaxRetries`），不是散落的魔法数字。

---

## 12. 多个 Worker 抢占任务时，如何避免长时间锁表、重复扫描和大量无效轮询？

**一句话：** 不在事务里做网络 I/O（CAS 只锁一条 UPDATE）；`ListDue` 只读不占；单批 100 条；`next_retry_at` 让失败消息退避；失败后立刻重建 channel 并跳出本批。

**逐项对应：**

| 问题 | 本项目的做法 | 位置 |
|---|---|---|
| 长时间锁表 | 不用 `FOR UPDATE` 包住 publish；只做单行 `UPDATE`，临界区是毫秒级 | `outbox_repo.go:59-78` |
| 重复扫描 | `ListDue` 只查不占，`Claim` 决定归属；`RowsAffected=0` 直接跳过 | `outbox_repo.go:42-50,74-76` |
| 无效轮询 | 1s 间隔；`next_retry_at` 退避；一次最多 100 条 | `outboxworker.go:19-20,93` |
| 坏连接放大失败 | 一条发布失败后 `Close()` 并置 `tmq=nil`，`break` 跳出本批，下一轮重建 | `outboxworker.go:158-162` |
| 多实例空转 | 每个实例 1 QPS 空 SELECT，走 `status` 索引，代价微秒级 | — |
| 表膨胀影响扫描 | `PurgeSent` 每 10min 清 24h 前的 `sent` | `outboxworker.go:84-91` |

**空轮询到底贵不贵？** 单实例每秒一次空 SELECT 几乎无感。真正的成本是：

1. **每条消息 3 次 DB 往返**：`Claim` → 发 MQ → `MarkSent`；100 条 = 200 次 UPDATE；
2. **多实例线性放大**：N 个 API 实例就是 N 倍空轮询，99% 空转；
3. **索引失效**：`next_retry_at IS NULL OR next_retry_at <= ?` 的 `OR + NULL` 很难走范围扫描，退避消息多时要逐行过滤才凑够 100 条；
4. **`failed` 堆积**拖慢扫描。

**优化路线（按 ROI 排序）：**

| 方案 | 收益 | 代价 |
|---|---|---|
| 自适应轮询：查到 0 条就指数放大间隔（1s→2s→4s→封顶 5s），有数据立刻回落 | 空闲期 DB 查询降到 1/5 | 延迟上界变 5s |
| 批量 Claim：短事务 `SKIP LOCKED` + 批量 UPDATE | 100 次 UPDATE → 2 次 SQL | 需 MySQL 8 |
| 批量 MarkSent：一批统一 `UPDATE ... WHERE id IN (...)` | 再省一半往返 | 要处理部分成功 |
| `visible_at` 哨兵值（默认 0）替掉 `OR IS NULL`，建 `(status, visible_at, id)` | 从逐行过滤变索引范围扫描 | 字段语义微调 |
| Redis 信号：提交后 `LPUSH`，poller `BRPOP`，另留 30s 兜底轮询 | 空闲期压力≈0、延迟毫秒级 | 信号会丢，必须兜底 |
| CDC（Canal/Debezium） | 毫秒级、无轮询 | 新组件 + 位点 + 高可用 |

**加分：** 不要一上来就分库分表。Outbox 稳态行数很低（发完就清），真正撑爆它的只有「长时间投递失败」这种异常场景——那本身就该告警，而不是靠扩容掩盖。

---

## 13. 消息积压时，如何判断是生产速度过快、消费者处理太慢，还是 RabbitMQ 自身发生故障？

**一句话：** 分三段看指标——**Outbox 侧看「待发数量 + 最老待发时长」**，**broker 侧看「ready/unacked + 发布/投递/确认速率 + 告警」**，**消费侧看「ACK 速率 + 处理耗时 + 错误率」**；再按「哪一段的速率塌了」定位。

**三段式决策表：**

| 现象 | 判断 | 处置 |
|---|---|---|
| `outbox_msgs` 里 `pending` 持续增长，但 broker 队列深度平稳 | 发布端问题：poller 没跑、channel 反复重建、confirm 超时 | 看 poller 日志、`retry_count` 分布、网络/MQ 连接状态 |
| broker `ready` 持续增长，消费 ACK 速率 < 发布速率 | 消费者跟不上：DB 慢、Redis 慢、handler 阻塞、QoS 太小/太大 | 扩消费者、查慢 SQL、查下游 RT、调 QoS |
| 发布速率和投递速率同时掉到 0，broker 有内存/磁盘告警、连接 blocked | broker 自身故障或限流 | 先救 broker（磁盘、内存、流控） |
| 发布速率本身突然升高（活动、刷量） | 真实流量突增 | 扩容消费者 + 生产者限流 |
| `retry_count` 分布整体偏高、`failed` 增长 | 毒消息或下游持续故障 | 区分可重试/不可重试，修数据后重放 |

**关键指标清单：**

**MySQL（Outbox）**

- `COUNT(*) WHERE status='pending'`：积压量；
- `MIN(create_time) WHERE status='pending'`：最老积压时长——比数量更能说明「链路断了多久」；
- `COUNT(*) WHERE status='processing' AND claimed_at < now()-60s`：卡死量；
- `COUNT(*) WHERE status='failed'`：终态失败，>0 就该告警；
- `MAX(retry_count)`：接近 5 说明即将转 failed。

**RabbitMQ**

- 每队列 `messages_ready` / `messages_unacknowledged`；
- `publish rate` / `deliver rate` / `ack rate`（管理 API 或 Prometheus exporter）；
- 连接/通道数、blocked 状态、内存与磁盘告警；
- 死信队列深度。

**消费端**

- 单条处理耗时 P50/P99、错误率、重试次数分布；
- 消费者实例数、QoS 预取、在途消息数。

**用 Little's Law 做交叉验证：** `积压 ≈ 到达速率 × 平均停留时间`。如果积压涨了但到达速率没变，说明停留时间变长——去查消费 RT；如果到达速率涨了、停留时间没变，就是单纯的生产变快。

**加分：** 不要只看队列深度。队列深度是结果，**速率和「最老消息年龄」才是原因**。另外，Outbox 的积压和 broker 的积压是两段独立的队列，必须分开看，否则会把「poller 挂了」误判成「消费者慢」。

---

# 第三层：故障与反问

## 14. MySQL 事务提交成功，但 RabbitMQ 持续不可用 2 小时，消息会发生什么？恢复后如何补发？

**一句话：** 消息安全留在 `outbox_msgs` 的 `pending` 里，poller 每 5s 尝试重建连接、不退出；MQ 恢复后自动补发，**不需要重启进程**。但「整机不可用」和「半死不活」要分开说，后者会烧重试额度。

**完整行为（`worker/outboxworker.go:59-69`）：**

```
tmq == nil
  → NewTimelineMQ 失败
  → 打日志，sleep 5s，continue   ← 回到循环顶部，下轮再试
```

关键细节：`continue` 发生在 `ListDue/Claim` **之前**，所以整机不可用期间：

- 消息不会被 `Claim`，不会进入 `processing`；
- `retry_count` **不会增长**，不会因为「停机 2 小时」把消息烧成 `failed`；
- 2 小时后消息仍然是 `pending`，恢复即被重新捞起。

这正是修复前的问题：旧代码启动时 `tmq == nil` 就 `return`，之后即使 MQ 恢复也永远不投递，只能重启 API。修成循环内重建后，才是真正的「降级不丢数据」。

**但要诚实区分两种「不可用」：**

| 故障形态 | 行为 | 风险 |
|---|---|---|
| 连接建立失败（broker 进程挂了/端口不通） | `NewTimelineMQ` 失败，不 Claim、不烧额度 | 安全，恢复即补发 |
| 连接在、但 publish NACK / confirm 5s 超时（磁盘告警、流控、网络黑洞） | 每条失败 → `MarkRetry`，5 次后转 `failed` | **约 62s 就会把消息打成终态**，恢复后不会自动重发 |

第二种是真实存在的边界：**当前代码把「连接级故障」和「单条消息故障」都算作一次重试**。更严谨的做法是分类错误：

- 连接/通道错误 → 不消耗重试额度（或重置 `retry_count`），只做退避重连；
- 协议级 NACK、消息体问题 → 才消耗额度并最终转 `failed`；
- 同时对 `failed` 增长做告警，并提供批量重置入口。

**恢复后补发节奏：**

- 每轮 `ListDue(100)` + 逐条 publish + confirm，轮与轮之间 1s；
- 粗略按 ~100 条/秒估算：2 小时、发布 10 条/秒 ≈ 7.2 万条，约 12 分钟排空；
- 排空期间消费端（QoS=10）可能成为新瓶颈，需要同步观察消费 ACK 速率。

**这 2 小时里用户看到什么？**

- 发布接口正常（写库成功即返回），数据不丢；
- 但热时间线里**没有新视频**——`ListLatest` 走 Redis 热区时只认 ZSET，ZSET 没更新就刷不到新内容；
- 恢复补发后，视频按原始 `create_time` 重新进入时间线，顺序不会错乱。

**加分：** 可以给读路径加一个「ZSET 最新 score 落后超过 N 秒就降级查 MySQL」的检测，避免发布侧故障时 Feed 长时间看不到新内容。这属于「派生数据过期」的读侧兜底，和写侧 Outbox 是两套互补机制。

---

## 15. 如果 RabbitMQ 已确认接收，但 Outbox 状态更新失败，如何处理？为什么这个场景通常无法实现严格的端到端恰好一次？

**一句话：** 只能按「至少一次」处理：本地行会在 60s 后被回收并重发，产生重复消息，由消费端幂等消化；严格的端到端 exactly-once 需要跨 DB 与 broker 的原子提交，代价不可接受。

**MarkSent 失败的分支：**

| 失败原因 | 事实状态 | 后续 |
|---|---|---|
| DB 不可用 / 超时 | broker 已收下，本地仍 `processing` | `ReapStuck` 回收 → 重发 → 重复 |
| `RowsAffected=0`（token 失效） | 已被别的实例接管 | 本次结果作废；新持有者会重发 → 重复 |
| 进程崩溃在 publish 与 MarkSent 之间 | 同上 | 同上 |

三种情况都无法区分「到底发没发成功」，所以只能选择**重发**。方向必须是「宁可重复，不可丢失」——这也是第 9 问里「副作用在前、记录在后」的原因。

**为什么严格端到端 exactly-once 做不到：**

1. **两个独立系统没有共同提交点**：MySQL 提交和 broker 接收无法原子完成；2PC 需要 broker 参与事务，RabbitMQ 不是 XA 资源；
2. **外部副作用不可回滚**：消息进了 broker 就撤不回来，不能像数据库写入那样 rollback；
3. **超时的本质是「状态未知」**：ACK 丢失时，发送方永远无法区分「没收到」和「收到了但确认丢了」；要消除歧义只能无限重试 + 无限保留，不现实（这也是「两将军问题」的工程版本）；
4. **即使投递恰好一次，消费也可能失败**：消费端处理到一半崩溃，broker 仍要重投。

所以工程上的正确目标不是「投递 exactly-once」，而是：

```
投递：at-least-once
效果：exactly-once（消费端幂等 / 去重表 / 状态机）
```

本项目的效果语义就是 exactly-once：`ZADD` 同一 member 是覆盖写，`score` 取不可变的 `create_time`，重复执行结果一致。

**如果业务方坚持要「严格恰好一次」怎么回答：**

- 先问清是「投递恰好一次」还是「效果恰好一次」——99% 的业务要的是后者；
- 效果恰好一次用「幂等键 + 唯一约束/去重表 + 与业务效果同事务」实现；
- 投递层面最多做到 at-least-once；要更强只能引入支持事务消息的 MQ 或 CDC，并且仍然要处理消费侧重复。

---

## 16. Outbox 表持续增长怎么办？归档、清理、分区和索引分别如何设计？

**一句话：** 先靠 `PurgeSent` 把存量压住（稳态行数只与投递速率相关），再靠复合索引保证扫描不劣化，最后才考虑归档/分区；**不建议一上来就分库分表**。

**第一层：控制存量（已实现）**

```
每 10 分钟：DELETE FROM outbox_msgs WHERE status='sent' AND sent_at < now()-24h
```

- 为什么保留 24h：留出审计、排查「这条到底发没发」的窗口；
- 稳态行数 ≈ `pending + processing + failed + sent(24h)`，与**投递速率**成正比，与历史总量无关；
- 如果审计要求低，保留窗口可以缩到 1h 甚至更短，直接降低存量。

**第二层：让查询不随总量劣化**

热查询是：

```sql
SELECT * FROM outbox_msgs
WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at <= ?)
ORDER BY id ASC LIMIT 100;
```

- 复合索引 `(status, next_retry_at, id)` 最优；退而求其次是 `status` + `next_retry_at` 两个单列索引；
- `CountPending` 走 `status` 索引，代价与表大小基本无关；
- `PurgeSent` 也走 `(status, sent_at)` 索引，避免全表扫；
- **注意 `OR ... IS NULL` 的坑**：很难走范围扫描。用 `visible_at NOT NULL DEFAULT 0` 哨兵值替代，条件改成 `visible_at <= ?`，就能稳定走索引。

**第三层：归档与终态**

- `failed` 是终态，长期堆积说明有系统性问题（毒消息、下游长期故障），应该**告警 + 人工处理 + 归档到历史表**，而不是留着让扫描变慢；
- 归档表可以只保留 `id/event_id/video_id/last_error/时间`，用于审计和重放。

**第四层：分区**

- 真到千万级，按 `create_time` 做 RANGE 分区，清理直接 `DROP PARTITION`，比 `DELETE` 快几个数量级，也不产生大量 undo/binlog；
- 代价：MySQL 要求**每个唯一键都包含分区列**，现有 `PRIMARY KEY(id)` 和 `UNIQUE(event_id)` 都要加上 `create_time`，属于 schema 级改动；
- 分区不是「性能银弹」，它主要解决**清理成本**和**单分区扫描量**，不解决查询本身写得烂的问题。

**顺手要补的两个细节：**

1. `PurgeSent` 目前是一条大 `DELETE`，存量很大时应改成**分批删除**（`LIMIT 1000` 循环），避免长事务、锁等待和主从延迟；
2. `PurgeSent` 只清 `sent`，`failed` 和长期 `pending` 需要有独立的处理策略，否则「清理」并不能真正控制总量。

**加分：** 「Outbox 的特点是稳态行数很低——消息发完就清。真正撑爆它的只有投递长时间失败这种异常场景，那应该告警，而不是靠扩容掩盖。」

---

## 17. 如果 Outbox 的 payload 是业务数据的快照，而业务数据随后发生变化，消费者应该使用事件快照还是重新查询数据库？

**一句话：** 取决于消费者要回答的是「当时发生了什么」还是「现在是什么状态」。**本项目走的是引用型事件**——不存 payload，只存 `video_id + create_time`，消费端只需要 ID，详情在读取时回源，所以不存在快照过期问题。

**先讲本项目的事实：**

`outbox_msgs` 没有 `payload` 列。事件体在发布时由行字段现场构造（`timelineMQ.go:99-104`）：

```json
{ "event_id": "...", "video_id": 987, "create_time": 1761300000000, "occurred_at": "..." }
```

消费端只做一件事：`ZADD feed:global_timeline score=create_time member=video_id`。标题、封面、点赞数这些字段**根本不在事件里**，而是在 Feed 读取时按 ID 从缓存/MySQL 取（`GetVideoByIDs`）。

**两种模型的取舍：**

| | 事件快照（payload 带全量字段） | 引用型（只带 ID / 关键字段） |
|---|---|---|
| 语义 | 忠实记录「事件发生那一刻」 | 只表达「发生了什么」，详情读最新 |
| 数据一致性 | 快照可能过期，消费端看到旧值 | 总是读到最新值 |
| 事件体积 | 大，表膨胀快 | 小，适合高频事件 |
| Schema 演进 | payload 结构变更要兼容老消息 | 不受业务字段变更影响 |
| 消费端成本 | 不用回源，快 | 需要回源，增加 DB/缓存压力 |
| 典型场景 | 审计、账单、通知文案、风控留痕 | 缓存失效、索引更新、时间线、计数触发 |

**判断标准（可以直接背）：**

- 需要「当时的值」→ 快照。比如账单金额、下单时的价格、通知正文，事后业务数据变了也不能改历史；
- 只需要「发生了什么、对象是谁」→ 引用。比如时间线更新、缓存失效、搜索索引重建；
- 两者都要 → **混合**：快照不可变的关键字段（金额、版本号、发生时间）+ 允许回源查询可变字段，并带上 `version` 让消费端判断新旧。

**混合模型的一个关键细节：** 如果消费端选择回源，必须处理「事件乱序 → 读到的是更新后的数据」的情况。比如「点赞 +1」和「取消点赞」两条事件乱序到达，回源读到的是最终点赞数，再按事件做增量就会算错。所以**增量类事件不能用回源值做增量**，要么快照增量值，要么用版本号/事件时间排序。

**本项目的已知边界：** 引用型事件 + ZSET 只存 ID，带来两个后果：

1. 视频被删除后，`video_id` 仍留在 ZSET 里，读取时靠 `GetVideoByIDs` 过滤，可能导致当页条数不足；
2. 消费端不感知「视频标题改了」这类变化——但时间线本来就不展示事件快照，所以无影响。

---

## 18. 如何保证同一个用户的通知事件有序？多个 Worker 并发发送时，数据库自增 ID 是否足以保证消息处理顺序？

**一句话：** 自增 ID 只保证**插入顺序**，不保证**投递顺序**，更不保证**处理顺序**；本项目不需要全局有序——时间线的顺序由 ZSET 的 `score=create_time` 重建，而不是由投递顺序决定。

**为什么自增 ID 不够：**

| 破坏顺序的因素 | 说明 |
|---|---|
| 多 Worker 并发抢占 | 虽然 `ListDue` 按 `id ASC` 取，但多个实例交错处理，A 拿了 1、3，B 拿了 2 |
| 重试 | 失败消息写入 `next_retry_at`，退避后重新可见，天然插到新消息后面 |
| 多消费者并发 | 同一个队列多个 consumer，prefetch 多条并行处理，完成顺序随机 |
| 自增 ID 本身 | InnoDB 自增值在插入时分配，事务提交顺序可能不同；回滚还会留空洞 |
| 网络/confirm | 发布成功但确认超时，重发会排到后面 |

所以「`id` 小的一定先被处理」这个假设在任何一步都不成立。

**本项目为什么不需要顺序：**

时间线是**按业务时间排序**的派生数据：

```go
ZAdd(key, Z{Score: float64(event.CreateTime), Member: videoID})
```

无论消息什么时候到、到几次，最终顺序都由不可变的 `create_time` 决定。这是把「投递顺序」这个难保证的性质，转换成「按业务字段排序」这个容易保证的性质——和幂等是同一类思路。

注意：ZSET score 相同时按 **member 字典序**排列，`"10" < "9"`，所以本项目用**毫秒时间戳**降低碰撞概率。真正做到严格稳定，还需要 `(create_time, id)` 复合排序，这是已知的简化项。

**如果确实需要「同一用户的通知有序」：**

1. **路由到单消费者**：按 `user_id` 做路由键，同一用户的消息进同一队列，并用 `x-single-active-consumer` 或「单消费者实例」保证串行；扩展性靠按用户哈希分片；
2. **按聚合根排序**：消息带 `aggregate_id + version`，消费端维护「已处理的最大版本」，旧版本直接丢弃或暂存重排；
3. **状态机 / 幂等**：很多「顺序问题」本质是「旧事件覆盖新状态」。用版本号做条件更新（`WHERE version < ?`）比追求投递顺序简单可靠；
4. **Kafka 分区**：按 `user_id` 分区，分区内有序；RabbitMQ 没有分区内严格有序的等价物（单队列单消费者可以，但吞吐受限）。

**关键区分：** 「消息有序」和「状态正确」是两件事。大多数业务要的是后者，用版本号/幂等就能解决，不必为全局有序付出单消费者的吞吐代价。

---

## 19. 如果 Outbox 消息最终进入死信状态，业务方如何发现、排查并人工补偿？人工重放会不会重复执行？

**一句话：** 本模块有**两个不同的「死信」**——发布端的 `outbox_msgs.status='failed'` 和消费端的 `<queue>.dlx`；前者靠 SQL 发现，后者靠队列深度/告警发现，重放都靠「重置状态」或「重新投递」，重复由幂等兜住。

**先分清两个终点：**

| | 发布端 failed | 消费端 DLQ |
|---|---|---|
| 触发条件 | 投递失败 5 次（或回收超额度） | 本地 4 次 + 重试队列 3 轮仍失败 |
| 存储 | `outbox_msgs` 表 | `video.timeline.update.queue.dlx` |
| 是否自动重试 | 否，终态 | 否，只告警 |
| 消息体 | `video_id + create_time + event_id` | 完整事件 JSON + `x-death` 头 |

**发现（当前能力 + 缺口）：**

```sql
-- 发布端：失败量、卡死量、积压时长
SELECT COUNT(*) FROM outbox_msgs WHERE status='failed';
SELECT COUNT(*) FROM outbox_msgs WHERE status='processing' AND claimed_at < now()-60s;
SELECT MIN(create_time) FROM outbox_msgs WHERE status='pending';
```

- 消费端：RabbitMQ 管理台/exporter 看 `<queue>.dlx` 的 `messages_ready`；
- **已知缺口**：`failed` 目前只打日志，没有告警和后台管理接口；DLQ 消费者目前也只打日志。这是文档里明确列出的待补项，面试时应主动说，而不是等面试官问出来。

**排查顺序：**

1. 看 `retry_count` 和最近日志里的错误：网络/超时/NACK 是环境问题，反序列化/字段非法是毒消息；
2. 看同一时间段 broker 是否有磁盘/内存告警、连接 blocked、队列被删；
3. 看消费端 DLQ 的 `x-death` 头：`reason`（rejected/expired）、`queue`、`count`，能直接定位是哪一级失败；
4. 看 Redis/MySQL 是否在故障窗口内（写 ZSET 失败会一路重试到 DLQ）；
5. 抽样核对 `video_id` 在 `videos` 表是否存在——不存在说明是脏事件，重放也没意义。

**人工补偿路径：**

**A. 发布端 `failed` 重放**

```sql
UPDATE outbox_msgs
SET status='pending', retry_count=0, next_retry_at=NULL,
    claimed_at=NULL, claim_token=NULL
WHERE id=? AND status='failed';
```

poller 下一轮就会捞走。因为 `event_id` 和 `create_time` 都没改：

- 消息体里的幂等键不变；
- ZSET 的 score 仍是原始发布时间，**重放不会把旧视频排到最前面**；
- 如果这条视频已经滚出最近 1000 条，`ZADD` 后会被 `ZREMRANGEBYRANK` 立刻裁掉——无害。

**B. 消费端 DLQ 重放**

把死信消息体重新发布到主交换机 + 原始 routing key。可以用管理台、`rabbitmqadmin` 或写一个小工具；重放前建议限速，避免 Redis/DB 被打出尖峰。

**C. 不补偿：重建热时间线**

ZSET 冷启动/被清空时本来就会从 MySQL 拉最新 1000 条重建。如果这条视频已经不在热区，它本来就不该出现，补不补无所谓。

**会不会重复执行？**

会。但本模块的副作用是 `ZADD` 覆盖写，重复执行结果一致：

- 发布端重放 → 消费端可能收到两条相同事件 → 同一 member 覆盖 → 无脏数据；
- 消费端 DLQ 重放 → 同上。

**如果是非幂等副作用（通知、计数），重放就会重复。** 那时的正确做法是：

- 消费端维护 `event_id` 去重表，唯一索引 + 与业务效果同事务；
- 或 Redis Lua 原子判重；
- 重放工具带审计日志（谁、何时、重放了哪些 `event_id`），避免「以为没重放，实际放了两遍」。

**回答的收尾口径：** 「死信不是丢弃，而是把失败变成可见状态。当前项目的缺口是缺告警和管理入口，不是缺数据——消息体和事件 ID 都在，随时可以重放。」

---

# 附：口径与边界（避免被绝对化表述反杀）

## A. 这个模块能承诺什么、不能承诺什么

| 可以说 | 不要说 |
|---|---|
| 业务数据与消息记录**同事务**，不会出现「有视频没消息」或「有消息没视频」 | 「消息绝对不丢」——broker 磁盘损坏、队列误删仍可能丢 |
| 投递是 **at-least-once**，重复由消费端幂等消化 | 「exactly-once 投递」 |
| 时间线更新是**幂等**的，重复执行结果一致 | 「端到端严格恰好一次」 |
| MQ 整机不可用期间**不丢数据**，恢复后自动补发 | 「任何 MQ 故障都能无损恢复」——半死不活时会烧重试额度 |
| 多实例下靠 CAS 把重复压到极低频 | 「CAS 保证不重复投递」——它只保证并发唯一 |
| 失败会进入 `failed`/DLQ，**可查可重放** | 「失败会自动兜底」——目前需要人工介入 |

## B. 被追问「你这套可靠性怎么证明」时的四层回答

```
① 不变量：业务行与 outbox 行同事务（可演示回滚）
② 故障注入：MQ 断开 / 杀进程 / 并发双实例 / Redis 宕机
③ 观测：pending 数、最老积压时长、processing 卡死数、failed 数
④ 残留风险：半死不活的 broker 会烧额度；failed/DLQ 缺告警入口
```

## C. 一页速查 SQL

```sql
-- 积压
SELECT COUNT(*) FROM outbox_msgs WHERE status='pending';
-- 最老积压
SELECT MIN(create_time) FROM outbox_msgs WHERE status='pending';
-- 卡死（超过租约）
SELECT COUNT(*) FROM outbox_msgs
WHERE status='processing' AND claimed_at < NOW() - INTERVAL 60 SECOND;
-- 终态失败
SELECT * FROM outbox_msgs WHERE status='failed' ORDER BY id DESC LIMIT 50;
-- 人工重放
UPDATE outbox_msgs
SET status='pending', retry_count=0, next_retry_at=NULL,
    claimed_at=NULL, claim_token=NULL
WHERE id=? AND status='failed';
```

## D. 一句话收尾

> 这个模块的核心不是「用了 Outbox」，而是三件事：**用同库事务把跨系统原子性降维**、**用幂等把不重复投递降级成重复无害**、**用显式状态机把失败变成可观测可干预**。confirm、CAS、fencing token、退避、自愈，都是这三件事的工程落地。
