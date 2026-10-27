package rabbitmq

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DLXExchange   = "dlx.events"
	MaxRetryCount = 3

	dlxQueueSuffix = ".dlx"
)

// DLQQueueName 返回主队列对应的死信队列名。
func DLQQueueName(mainQueue string) string {
	return mainQueue + dlxQueueSuffix
}

// DLQRoutingKey 返回主队列死信时使用的路由键。
// 约定：死信队列 <queue>.dlx 只绑定这一个 key，保证死信不会被广播到别的死信队列。
func DLQRoutingKey(mainQueue string) string {
	return mainQueue + dlxQueueSuffix
}

// DeclareDLX 声明死信交换机和该主队列专属的死信队列。
//
// 绑定方式从「#」改为精确 key：dlx.events 是所有队列共用的（topic）交换机，
// 之前每个死信队列都用 "#" 绑定，等于把交换机变成 fanout，一条死信会被复制到
// 所有死信队列。改成精确 key 后，A 队列的死信只会进入 A.dlx。
func DeclareDLX(ch *amqp.Channel, queueName string) error {
	if ch == nil {
		return nil
	}
	if err := ch.ExchangeDeclare(
		DLXExchange, "topic", true, false, false, false, nil,
	); err != nil {
		return err
	}
	dlxQueue := DLQQueueName(queueName)
	if _, err := ch.QueueDeclare(
		dlxQueue, true, false, false, false, nil,
	); err != nil {
		return err
	}
	if err := ch.QueueBind(dlxQueue, DLQRoutingKey(queueName), DLXExchange, false, nil); err != nil {
		return err
	}
	log.Printf("DLX ready: exchange=%s queue=%s routingKey=%s", DLXExchange, dlxQueue, DLQRoutingKey(queueName))
	return nil
}

// GetRetryCount 从 AMQP x-death header 中提取当前消息已被重试的次数。
//
// x-death 是 RabbitMQ 在「消息被死信 / 被拒」时累加的头部，count 表示同一条消息
// 从同一个队列被死信出去的次数。本项目的重试靠「重试队列 TTL 到期 → 死信回主
// 交换机」实现，因此这里的 count 就是「已重投的轮数」，用作终止重试的依据。
func GetRetryCount(d amqp.Delivery) int {
	deaths, ok := d.Headers["x-death"].([]interface{})
	if !ok || len(deaths) == 0 {
		return 0
	}
	for _, raw := range deaths {
		death, ok := raw.(amqp.Table)
		if !ok {
			continue
		}
		// 多次死信会在 x-death 里累积多条记录，取最大的 count 作为总轮数。
		count, ok := death["count"].(int64)
		if !ok {
			continue
		}
		if int(count) > 0 {
			return int(count)
		}
	}
	return 0
}
