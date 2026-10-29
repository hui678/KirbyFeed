package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/video"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	outboxPollInterval   = time.Second // 正常轮询间隔：这就是"秒级投递延迟"的来源
	outboxPollBatch      = 100
	outboxReapInterval   = 30 * time.Second // 回收崩溃残留的 processing 消息
	outboxPurgeInterval  = 10 * time.Minute // 清理已发送的历史消息
	outboxRetryWait      = 5 * time.Second  // MQ/Channel 未就绪时的重连等待
	outboxStatsInterval  = time.Minute      // 周期打印状态，供日志采集做告警
	outboxPendingWarnAge = 5 * time.Minute  // 最老 pending 超过这个时长就告警
)

// StartOutboxPoller 后台轮询本地消息表，把"已随业务事务提交、但还没投递"的
// 视频发布事件发到 RabbitMQ，消费端再写入 Redis 热时间线。
//
// 四处关键可靠性设计：
//  1. publisher confirm：只有 broker 返回 ack，消息才会被置为 sent（PublishWithDeferredConfirm + DeliveryTag）；
//  2. CAS 抢占 + fencing token：多实例下靠 UPDATE ... WHERE status='pending' 的 RowsAffected
//     决定归属；令牌保证「卡住的旧持有者」不会把「新持有者」的消息标成已发送；
//  3. 稳定的 event_id：幂等键在写入 outbox 时就固定，重投不换 ID，消费端才可能去重；
//  4. 自愈：MQ 不可用时 poller 不退出，周期性重建 TimelineMQ，连接恢复后自动补发积压消息。
//
// 已知取舍：轮询带来约 1s 的投递延迟，换来"发布写库与消息投递的原子性"。
func StartOutboxPoller(ctx context.Context, repo *video.OutboxRepository, rmq *rabbitmq.RabbitMQ) {
	if repo == nil {
		log.Printf("Outbox poller disabled: outbox repository is nil")
		return
	}

	go func() {
		var tmq *rabbitmq.TimelineMQ
		// 刻意用零值：启动后第一轮立刻做一次回收，把"上次进程崩溃留下的 processing 消息"
		// 马上接管过来。如果初始化成 time.Now()，重启后要白等 30 秒才开始接管。
		var lastReap time.Time
		var lastPurge time.Time
		var lastStats time.Time

		for {
			select {
			case <-ctx.Done():
				_ = tmq.Close()
				log.Printf("Outbox poller stopped")
				return
			default:
			}

			// ---- 自愈：MQ 没准备好就周期性重试，而不是启动失败后永久退出 ----
			if tmq == nil {
				t, err := rabbitmq.NewTimelineMQ(rmq)
				if err != nil {
					log.Printf("Outbox poller: TimelineMQ 未就绪(%v), %v 后重试", err, outboxRetryWait)
					sleepOrDone(ctx, outboxRetryWait)
					continue
				}
				tmq = t
				log.Printf("Outbox poller: TimelineMQ 已就绪")
			}

			now := time.Now()

			// 周期性维护：回收崩溃残留 + 清理历史消息
			if now.Sub(lastReap) >= outboxReapInterval {
				res, err := repo.ReapStuck(ctx, now.Add(-video.OutboxClaimTimeout))
				if err != nil {
					log.Printf("Outbox poller: 回收超时消息失败: %v", err)
				} else if res.Reclaimed > 0 || res.Failed > 0 {
					log.Printf("Outbox poller: 回收卡住的消息 %d 条, 转 failed %d 条",
						res.Reclaimed, res.Failed)
				}
				lastReap = now
			}
			if now.Sub(lastPurge) >= outboxPurgeInterval {
				if n, err := repo.PurgeSent(ctx, now.Add(-video.OutboxSentRetention)); err != nil {
					log.Printf("Outbox poller: 清理历史消息失败: %v", err)
				} else if n > 0 {
					log.Printf("Outbox poller: 清理 %d 条已发送的历史消息", n)
				}
				lastPurge = now
			}

			// 周期性输出状态：failed/stuck/最老 pending 超阈值时打 [ALERT]，
			// 让「消息卡住了」在没有管理台的情况下也能被日志采集发现。
			if now.Sub(lastStats) >= outboxStatsInterval {
				stats, err := repo.Stats(ctx, now.Add(-video.OutboxClaimTimeout))
				if err != nil {
					log.Printf("Outbox poller: 状态统计失败: %v", err)
				} else {
					logOutboxStats(now, stats)
				}
				lastStats = now
			}

			msgs, err := repo.ListDue(ctx, outboxPollBatch, now)
			if err != nil {
				log.Printf("Outbox poller: 查询待发消息失败: %v, %v 后重试", err, outboxPollInterval)
				sleepOrDone(ctx, outboxPollInterval)
				continue
			}
			if len(msgs) == 0 {
				sleepOrDone(ctx, outboxPollInterval)
				continue
			}

			for i := range msgs {
				msg := msgs[i]

				// CAS 抢占 + fencing token：多实例下只有一个实例能拿到 ok=true
				token, claimed, err := repo.Claim(ctx, msg.ID, now)
				if err != nil {
					log.Printf("Outbox poller: 抢占消息失败: id=%d, err=%v", msg.ID, err)
					continue
				}
				if !claimed {
					continue // 已被其他实例抢走
				}

				// 带上 outbox 行里持久化的 event_id：重投时 ID 不变，消费端才可能靠它去重。
				eventID := ""
				if msg.EventID != nil {
					eventID = *msg.EventID
				}

				pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				pubErr := tmq.PublishVideo(pubCtx, msg.VideoID, msg.CreateTime, eventID)
				cancel()

				if pubErr == nil {
					// token 校验不通过 = 这条消息已被别的实例回收并重新抢占，
					// 本次投递结果必须作废，否则会把别人还没发成功的消息标成 sent。
					ok, err := repo.MarkSent(ctx, msg.ID, token, time.Now())
					if err != nil {
						log.Printf("Outbox poller: 标记 sent 失败: id=%d, err=%v", msg.ID, err)
					} else if !ok {
						log.Printf("Outbox poller: id=%d 已被其他实例接管, 本次投递结果丢弃", msg.ID)
					}
					continue
				}

				log.Printf("Outbox poller: 投递失败: id=%d video=%d retry=%d err=%v",
					msg.ID, msg.VideoID, msg.RetryCount, pubErr)

				// broker/连接级故障：消息本身没问题，不消耗重试额度。
				// MQ 长时间不可用期间消息会一直 pending，恢复后自动补发；
				// 永久性拓扑损坏由「最老 pending 时长」告警发现，而不是靠烧重试次数。
				if errors.Is(pubErr, rabbitmq.ErrBrokerUnavailable) {
					ok, err := repo.MarkDeferred(ctx, msg.ID, token,
						time.Now(), video.OutboxBrokerRetryDelay, errText(pubErr))
					if err != nil {
						log.Printf("Outbox poller: 安排重投失败: id=%d, err=%v", msg.ID, err)
					} else if !ok {
						log.Printf("Outbox poller: id=%d 已被其他实例接管, 本次重投安排丢弃", msg.ID)
					}

					// 连接级故障大概率意味着整条 channel 都不可用：
					// 重建一次，避免整批消息反复撞同一条坏连接。
					_ = tmq.Close()
					tmq = nil
					break
				}

				// 消息级失败：消耗重试额度，耗尽后转 failed 等人工处理。
				if msg.RetryCount+1 > video.OutboxMaxRetries {
					ok, err := repo.MarkFailed(ctx, msg.ID, token, errText(pubErr))
					if err != nil {
						log.Printf("Outbox poller: 标记 failed 失败: id=%d, err=%v", msg.ID, err)
					} else if ok {
						log.Printf("Outbox poller: id=%d 超过最大重试次数(%d), 转入 failed 等待人工处理",
							msg.ID, video.OutboxMaxRetries)
					}
					continue
				}
				if ok, err := repo.MarkRetry(ctx, msg.ID, token, msg.RetryCount+1, time.Now(), errText(pubErr)); err != nil {
					log.Printf("Outbox poller: 安排重试失败: id=%d, err=%v", msg.ID, err)
				} else if !ok {
					log.Printf("Outbox poller: id=%d 已被其他实例接管, 本次重试安排丢弃", msg.ID)
				}
				// NACK 是消息级问题，不代表 channel 坏了：继续处理本批，不重建连接。
			}

			sleepOrDone(ctx, outboxPollInterval)
		}
	}()
}

// StartConsumer 消费时间线事件，把 video_id 以 create_time 为 score 写入 Redis ZSET，
// 并且只保留最近 1000 条（滚动淘汰），构成"热时间线"。
//
// 与 Outbox poller 一样，MQ 未就绪时不再直接 return，而是进入重连循环，恢复后自动接管。
//
// 容错统一交给 Consumer 骨架（进程内退避重试 → 重试队列延迟重投 → 死信队列告警）：
// 之前这里是「重试 3 次后 Ack 丢弃」，等于把 Outbox 辛辛苦苦保证的「至少一次投递」
// 在最后一环丢掉了 —— 时间线少一条没有任何报错，是最难排查的一类故障。
func StartConsumer(ctx context.Context, rmq *rabbitmq.RabbitMQ, queueName string, redisClient *rediscache.Client, retry RetryPublisher) {
	if redisClient == nil {
		log.Printf("Timeline consumer disabled: redis is not initialized")
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("Timeline consumer stopped")
				return
			default:
			}

			if rmq == nil || rmq.Conn == nil {
				log.Printf("Timeline consumer: rabbitmq 未就绪, %v 后重试", outboxRetryWait)
				sleepOrDone(ctx, outboxRetryWait)
				continue
			}

			// 每次重连创建独立的 Channel，不与发布者共用
			ch, err := rmq.NewChannel()
			if err != nil {
				log.Printf("Timeline consumer: 创建 Channel 失败: %v, %v 后重试", err, outboxRetryWait)
				sleepOrDone(ctx, outboxRetryWait)
				continue
			}

			// 确保交换机/队列/绑定存在（幂等），避免消费者先于发布者启动时 Consume 404
			if err := rabbitmq.DeclareTimelineTopic(ch); err != nil {
				log.Printf("Timeline consumer: 声明拓扑失败: %v, %v 后重试", err, outboxRetryWait)
				ch.Close()
				sleepOrDone(ctx, outboxRetryWait)
				continue
			}

			if err := ch.Qos(10, 0, false); err != nil {
				log.Printf("Timeline consumer: QoS 设置失败: %v", err)
			}

			c := &Consumer{
				Name:  "TimelineConsumer",
				Queue: queueName,
				Retry: retry,
				// 时间线在 QoS=10 的单条流里消费：进程内 sleep 重试会阻塞整条流。
				// 只留 1 次本地重试抹平瞬时抖动，其余尽早交给 TTL 重试队列。
				MaxLocalRetries: 1,
				Process: func(ctx context.Context, d amqp.Delivery) error {
					return processTimeline(ctx, redisClient, d)
				},
			}

			log.Printf("Timeline consumer 已启动, queue=%s", queueName)
			if err := c.Run(ctx, ch); err != nil {
				if ctx.Err() != nil {
					ch.Close()
					return
				}
				log.Printf("Timeline consumer: %v, %v 后重连...", err, outboxRetryWait)
			}

			// Channel 断开或消费循环退出，关闭后重连
			ch.Close()
			sleepOrDone(ctx, outboxRetryWait)
		}
	}()
}

// processTimeline 处理一条时间线事件，返回 error 交给 Consumer 骨架决定是否重试。
func processTimeline(ctx context.Context, redisClient *rediscache.Client, d amqp.Delivery) error {
	var event rabbitmq.TimelineEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		// 结构性坏消息：重试多少次都解不开，直接丢弃（返回 nil 走 Ack）。
		log.Printf("Timeline consumer: 反序列化失败: %v", err)
		return nil
	}
	if event.VideoID == 0 {
		return nil
	}
	if err := zaddTimeline(ctx, redisClient, &event); err != nil {
		log.Printf("Timeline consumer: 写入 ZSET 失败: video=%d event=%s err=%v",
			event.VideoID, event.EventID, err)
		return err
	}
	return nil
}

// zaddTimeline 单次写入。ZSET 的 member 是 video_id，重复写入是幂等覆盖，
// 所以"至少一次投递"不会造成脏数据——这正是 Outbox 可以放心重试的前提。
//
// 关于「要不要用 event_id 做阻断式去重」：这里**刻意不做**。
//   - 去重必须「先占位再执行」才有拦截意义，而一旦占位后写 ZSET 失败，
//     后续重试会被占位挡住，这条时间线就永久少一条 —— 去重反而制造了丢数据。
//   - 若改成「执行成功后再标记」，标记就失去了拦截作用，等于没做。
//   - 所以本场景的正确解法是把幂等下沉到存储操作本身（ZADD 是覆盖语义），
//     而不是在消费端加一层有状态拦截。
//
// event_id 的价值在于：它从写入 outbox 那一刻起就固定下来，重投不换 ID，
// 因此日志可以按它追踪"同一条事件被投递了几次"；将来若给时间线加入非幂等副作用
// （例如通知粉丝、计数累加），它也是唯一可用的去重键。
func zaddTimeline(ctx context.Context, redisClient *rediscache.Client, event *rabbitmq.TimelineEvent) error {
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// 写入与裁剪必须原子：分成两条命令时，中间会露出 1001 条的窗口；
	// 更糟的是裁剪失败原先只打日志，集合会一直超量到下一次成功写入。
	// Lua 在 Redis 内单线程执行，不存在「写进去了但没裁」的中间态。
	_, err := redisClient.Eval(opCtx, timelineZAddAndTrim, []string{video.TimelineZSetKey(redisClient)},
		float64(event.CreateTime),
		fmt.Sprintf("%d", event.VideoID),
		video.TimelineCapacity,
	)
	return err
}

// timelineZAddAndTrim 写入一条时间线事件并按排名裁剪，返回裁剪后的基数。
// ARGV: [score, member, capacity]
const timelineZAddAndTrim = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[3]) + 1))
return redis.call('ZCARD', KEYS[1])
`

// logOutboxStats 周期输出 Outbox 状态，异常时打 [ALERT] 前缀，
// 方便日志采集按关键字告警（这是没有管理台时的最小观测手段）。
// 同一份数据也写成 gauge，便于在 /metrics 上画「pending 是否持续上涨」。
func logOutboxStats(now time.Time, s video.OutboxStats) {
	observability.SetGauge("feedsystem_outbox_messages", s.Pending, "status", "pending")
	observability.SetGauge("feedsystem_outbox_messages", s.Processing, "status", "processing")
	observability.SetGauge("feedsystem_outbox_messages", s.Failed, "status", "failed")
	observability.SetGauge("feedsystem_outbox_messages", s.Stuck, "status", "stuck")
	if s.OldestPendingAt != nil {
		observability.SetGauge("feedsystem_outbox_oldest_pending_seconds",
			int64(now.Sub(*s.OldestPendingAt).Seconds()))
	} else {
		observability.SetGauge("feedsystem_outbox_oldest_pending_seconds", 0)
	}

	if s.Pending == 0 && s.Processing == 0 && s.Failed == 0 && s.Stuck == 0 {
		return
	}
	oldest := "-"
	if s.OldestPendingAt != nil {
		oldest = now.Sub(*s.OldestPendingAt).Round(time.Second).String()
	}
	log.Printf("Outbox stats: pending=%d processing=%d failed=%d stuck=%d max_retry=%d oldest_pending=%s",
		s.Pending, s.Processing, s.Failed, s.Stuck, s.MaxRetryCount, oldest)

	if s.Failed > 0 {
		log.Printf("[ALERT] Outbox 有 %d 条 failed，需要人工排查并重放（POST /admin/outbox/replay）", s.Failed)
	}
	if s.Stuck > 0 {
		log.Printf("[ALERT] Outbox 有 %d 条 processing 超过 %s 未落地，检查是否有实例崩溃",
			s.Stuck, video.OutboxClaimTimeout)
	}
	if s.OldestPendingAt != nil {
		if age := now.Sub(*s.OldestPendingAt); age > outboxPendingWarnAge {
			log.Printf("[ALERT] Outbox 最老 pending 已等待 %s（阈值 %s），投递链路可能中断",
				age.Round(time.Second), outboxPendingWarnAge)
		}
	}
}

// errText 截断错误信息，避免超出 last_error 列的长度（按 rune 截断，不切坏 UTF-8）。
func errText(err error) string {
	if err == nil {
		return ""
	}
	r := []rune(err.Error())
	const maxLen = 500
	if len(r) > maxLen {
		r = r[:maxLen]
	}
	return string(r)
}

// sleepOrDone 可中断的 sleep，保证进程退出时 goroutine 不会卡在等待里
func sleepOrDone(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
