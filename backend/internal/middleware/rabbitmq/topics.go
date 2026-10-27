package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Topic 描述一类事件用到的交换机 / 主队列 / 绑定键。
//
// 存在的意义是消除重复：之前 Worker 进程（cmd/worker/main.go）和 rabbitmq 包各写了一份
// 交换机/队列常量，改一处忘一处就会出现「worker 声明在 A 队列、发布端发到 B 交换机」的
// 静默故障。现在只有这里一份定义，两边都从这里取。
type Topic struct {
	Name       string
	Exchange   string
	Queue      string
	BindingKey string
}

var (
	LikeTopic = Topic{
		Name:       "like",
		Exchange:   likeExchange,
		Queue:      likeQueue,
		BindingKey: likeBindingKey,
	}
	CommentTopic = Topic{
		Name:       "comment",
		Exchange:   commentExchange,
		Queue:      commentQueue,
		BindingKey: commentBindingKey,
	}
	SocialTopic = Topic{
		Name:       "social",
		Exchange:   socialExchange,
		Queue:      socialQueue,
		BindingKey: socialBindingKey,
	}
	PopularityTopic = Topic{
		Name:       "popularity",
		Exchange:   popularityExchange,
		Queue:      popularityQueue,
		BindingKey: popularityBindingKey,
	}
	// TimelineTopic 由 Outbox poller 发布、时间线消费者消费。
	// 它和上面四类事件一样需要重试队列与死信队列，Worker 进程不消费它（在 API 进程内消费）。
	TimelineTopic = Topic{
		Name:       "timeline",
		Exchange:   timelineExchange,
		Queue:      timelineQueue,
		BindingKey: timelineBindingKey,
	}
)

// Declare 声明主队列拓扑 + 专属死信队列 + 延迟重试队列，一次到位。
func (t Topic) Declare(ch *amqp.Channel, retryTTL time.Duration) error {
	if err := DeclareTopic(ch, t.Exchange, t.Queue, t.BindingKey); err != nil {
		return err
	}
	return DeclareRetry(ch, t.Exchange, t.Queue, retryTTL)
}
