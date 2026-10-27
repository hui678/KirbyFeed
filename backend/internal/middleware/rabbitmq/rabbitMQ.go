package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/config"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ 只管理 Connection，Channel 由各组件按需创建
type RabbitMQ struct {
	Conn *amqp.Connection
}

func NewRabbitMQ(cfg *config.RabbitMQConfig) (*RabbitMQ, error) {
	if cfg == nil {
		return nil, errors.New("rabbitmq config is nil")
	}
	url := "amqp://" + cfg.Username + ":" + cfg.Password + "@" + cfg.Host + ":" + strconv.Itoa(cfg.Port) + "/"
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return &RabbitMQ{Conn: conn}, nil
}

func (r *RabbitMQ) Close() error {
	if r == nil {
		return nil
	}
	if r.Conn != nil {
		return r.Conn.Close()
	}
	return nil
}

func (r *RabbitMQ) NewChannel() (*amqp.Channel, error) {
	if r == nil || r.Conn == nil {
		return nil, errors.New("rabbitmq connection is not initialized")
	}
	return r.Conn.Channel()
}

func DeclareTopic(ch *amqp.Channel, exchange string, queue string, bindingKey string) error {
	if ch == nil {
		return errors.New("channel is not initialized")
	}
	if exchange == "" || queue == "" || bindingKey == "" {
		return errors.New("exchange/queue/bindingKey is required")
	}

	if err := ch.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	// 死信交换机 + 死信路由键一起指定：死信交换机是所有主队列共用的 dlx.events，
	// 如果只给交换机不给路由键，死信会沿用原始 routing key（如 like.like），
	// 而每个 <queue>.dlx 又都用 "#" 绑定在同一个 exchange 上，结果一条死信被
	// 复制到所有死信队列，可能被不相干的消费者处理。这里改成「每个主队列一个专属
	// 死信路由键」，让死信只落到它自己的死信队列。
	q, err := ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    DLXExchange,
			"x-dead-letter-routing-key": DLQRoutingKey(queue),
		},
	)
	if err != nil {
		return err
	}

	if err := ch.QueueBind(
		q.Name,
		bindingKey,
		exchange,
		false,
		nil,
	); err != nil {
		return err
	}
	if err := DeclareDLX(ch, queue); err != nil {
		log.Printf("DLX declare failed for %s: %v", queue, err)
	}
	return nil
}

func PublishJSON(ctx context.Context, ch *amqp.Channel, exchange string, routingKey string, payload any) error {
	if ch == nil {
		return errors.New("channel is not initialized")
	}
	if exchange == "" || routingKey == "" {
		return errors.New("exchange and routingKey are required")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         b,
	})
}

func newEventID(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// NewEventID 生成一个事件 ID。服务层需要它来做「发布前先定 ID」的幂等设计：
// 同一条业务事件无论走 MQ 还是降级直写，都携带同一个 event_id，
// 这样消费端才能靠唯一索引去重。
func NewEventID() (string, error) {
	return newEventID(16)
}
