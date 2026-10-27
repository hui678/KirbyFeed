package video

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"gorm.io/gorm"
)

// outboxReapBatch 单次回收的条数上限，避免一次大事务锁住整张表。
const outboxReapBatch = 1000

// newClaimToken 生成一次抢占的 fencing 令牌。
// 只需要「每次抢占都不同」，所以用随机串即可，不需要全局有序。
func newClaimToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// OutboxRepository 负责本地消息表（Outbox）的状态流转。
//
// 并发安全的设计取舍：这里**没有**用 SELECT ... FOR UPDATE SKIP LOCKED，
// 而是用「条件更新 + RowsAffected」的 CAS 来抢占消息。原因是 SKIP LOCKED 必须把
// "发布消息到 RabbitMQ" 这段网络 I/O 包在数据库事务里，事务会长达几十毫秒甚至更久；
// CAS 只在一条 UPDATE 上加锁，拿到发送权后再慢慢发，吞吐和锁竞争都更健康。
// 两者在多实例下的安全性等价：都是"只有一个实例能把状态从 pending 改走"。
type OutboxRepository struct {
	db *gorm.DB
}

func NewOutboxRepository(db *gorm.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// ListDue 取出"已到期待发"的消息。只查询不占用，归属权由 Claim 决定。
func (r *OutboxRepository) ListDue(ctx context.Context, limit int, now time.Time) ([]OutboxMsg, error) {
	var msgs []OutboxMsg
	err := r.db.WithContext(ctx).
		Where("status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?)", OutboxStatusPending, now).
		Order("id ASC").
		Limit(limit).
		Find(&msgs).Error
	return msgs, err
}

// Claim 抢占一条消息：只有把 status 从 pending 改成 processing 的那个实例拿到发送权。
// 多实例部署时其余实例会拿到 ok=false，直接跳过，天然去重。
//
// 返回的 token 是 fencing 令牌，必须原样带回给 MarkSent/MarkRetry/MarkFailed：
// 「worker A 卡住超时 → 被 ReapStuck 回收 → worker B 抢占（新 token）→ A 恢复」时，
// A 手里是旧 token，它的状态更新会被 WHERE claim_token = ? 挡掉（返回 false）。
// 没有这一步，A 会把 B 正在处理的消息标成 sent —— 消息其实还没发成功，直接丢失。
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
		return "", false, nil
	}
	return token, true, nil
}

// MarkSent 投递成功（broker 已 confirm 落盘），进入终态 sent。
// 返回 false 表示令牌已失效：消息被别的实例回收并重新抢占了，本次结果不能落地。
func (r *OutboxRepository) MarkSent(ctx context.Context, id uint, token string, now time.Time) (bool, error) {
	return r.updateWithToken(ctx, id, token, map[string]interface{}{
		"status":      OutboxStatusSent,
		"sent_at":     now,
		"claim_token": nil,
	})
}

// MarkRetry 投递失败但仍有重试额度：退回 pending，按指数退避安排下一次可见时间。
// newRetryCount 是"这次失败之后的累计重试次数"。
func (r *OutboxRepository) MarkRetry(ctx context.Context, id uint, token string, newRetryCount int, now time.Time) (bool, error) {
	return r.updateWithToken(ctx, id, token, map[string]interface{}{
		"status":        OutboxStatusPending,
		"claimed_at":    nil,
		"claim_token":   nil,
		"retry_count":   newRetryCount,
		"next_retry_at": now.Add(OutboxBackoff(newRetryCount)),
	})
}

// MarkFailed 重试额度耗尽，进入终态 failed，不再自动重试，等人工介入。
func (r *OutboxRepository) MarkFailed(ctx context.Context, id uint, token string) (bool, error) {
	return r.updateWithToken(ctx, id, token, map[string]interface{}{
		"status":      OutboxStatusFailed,
		"claimed_at":  nil,
		"claim_token": nil,
	})
}

// updateWithToken 是 MarkSent/MarkRetry/MarkFailed 的公共实现：
// 状态迁移只在「仍是 processing 且 claim_token 未被刷新」时生效。
func (r *OutboxRepository) updateWithToken(ctx context.Context, id uint, token string, values map[string]interface{}) (bool, error) {
	if token == "" {
		return false, errors.New("claim token is required")
	}
	tx := r.db.WithContext(ctx).Model(&OutboxMsg{}).
		Where("id = ? AND status = ? AND claim_token = ?", id, OutboxStatusProcessing, token).
		Updates(values)
	if tx.Error != nil {
		return false, tx.Error
	}
	return tx.RowsAffected == 1, nil
}

// ReapResult 是 ReapStuck 的结果。
type ReapResult struct {
	Reclaimed int64 // 被放回 pending、等待其他实例重新抢占的数量
	Failed    int64 // 连回收额度都耗尽、转 failed 等人工介入的数量
}

// ReapStuck 回收「被抢占后进程崩溃/卡死」的消息：claimed_at 早于 before 就放回 pending。
// 没有这一步，抢占者在 processing 阶段挂掉的消息会永远没人管。
//
// 两个容易被忽略的点：
//  1. 回收时**必须清掉 claim_token**。原持有者如果其实只是卡住（没死），它恢复后
//     带着旧 token 调 MarkXxx 会被 updateWithToken 挡掉，不会误改别人的状态。
//  2. 回收**同样消耗一次重试额度**。否则「抢占 → 崩溃 → 回收 → 抢占」可以无限循环，
//     这条消息永远在 pending/processing 之间打转，而且没有任何告警出口。
func (r *OutboxRepository) ReapStuck(ctx context.Context, before time.Time) (ReapResult, error) {
	var res ReapResult

	var rows []struct {
		ID         uint
		RetryCount int
	}
	if err := r.db.WithContext(ctx).Model(&OutboxMsg{}).
		Select("id", "retry_count").
		Where("status = ? AND claimed_at IS NOT NULL AND claimed_at < ?", OutboxStatusProcessing, before).
		Order("id ASC").
		Limit(outboxReapBatch).
		Find(&rows).Error; err != nil {
		return res, err
	}

	for _, row := range rows {
		n := row.RetryCount + 1
		if n > OutboxMaxRetries {
			tx := r.db.WithContext(ctx).Model(&OutboxMsg{}).
				Where("id = ? AND status = ?", row.ID, OutboxStatusProcessing).
				Updates(map[string]interface{}{
					"status":      OutboxStatusFailed,
					"claimed_at":  nil,
					"claim_token": nil,
					"retry_count": n,
				})
			if tx.Error != nil {
				return res, tx.Error
			}
			res.Failed += tx.RowsAffected
			continue
		}

		tx := r.db.WithContext(ctx).Model(&OutboxMsg{}).
			Where("id = ? AND status = ?", row.ID, OutboxStatusProcessing).
			Updates(map[string]interface{}{
				"status":        OutboxStatusPending,
				"claimed_at":    nil,
				"claim_token":   nil,
				"retry_count":   n,
				"next_retry_at": nil, // 立刻可见，不用再等一轮退避
			})
		if tx.Error != nil {
			return res, tx.Error
		}
		res.Reclaimed += tx.RowsAffected
	}
	return res, nil
}

// PurgeSent 清理已发送的历史消息，避免 outbox 表无限增长。
func (r *OutboxRepository) PurgeSent(ctx context.Context, before time.Time) (int64, error) {
	tx := r.db.WithContext(ctx).
		Where("status = ? AND sent_at < ?", OutboxStatusSent, before).
		Delete(&OutboxMsg{})
	return tx.RowsAffected, tx.Error
}

// CountPending 供日志观测使用：当前积压了多少条未投递消息。
func (r *OutboxRepository) CountPending(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&OutboxMsg{}).
		Where("status = ?", OutboxStatusPending).
		Count(&n).Error
	return n, err
}
