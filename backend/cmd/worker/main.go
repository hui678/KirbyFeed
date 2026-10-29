package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	mqrabbit "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"
)

// 本进程消费的四类事件。拓扑（交换机 / 队列 / 绑定键 / 死信 / 重试队列）统一从
// rabbitmq 包的 Topic 注册表取，这里只负责把它们装配成 Worker。
var topics = []mqrabbit.Topic{
	mqrabbit.LikeTopic,
	mqrabbit.CommentTopic,
	mqrabbit.SocialTopic,
	mqrabbit.PopularityTopic,
}

func connectWithRetry(name string, maxRetries int, fn func() error) {
	for i := 0; i < maxRetries; i++ {
		if err := fn(); err == nil {
			return
		}
		wait := time.Duration(1<<i) * time.Second
		if wait > 30*time.Second {
			wait = 30 * time.Second
		}
		log.Printf("%s 不可用，%v 后重试 (%d/%d)...", name, wait, i+1, maxRetries)
		time.Sleep(wait)
	}
	log.Fatalf("%s: 超过最大重试次数", name)
}

// runWorkerWithRetry 为每个 Worker 创建独立 Channel，断开后自动重连
func runWorkerWithRetry(ctx context.Context, name string, conn *amqp.Connection, fn func(*amqp.Channel) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("%s: 创建 Channel 失败: %v, 5秒后重试", name, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if err := ch.Qos(50, 0, false); err != nil {
			log.Printf("%s: QoS 设置失败: %v", name, err)
		}

		log.Printf("%s started, consuming", name)
		if err := fn(ch); err != nil {
			if ctx.Err() != nil {
				ch.Close()
				return
			}
			log.Printf("%s: %v, 5秒后重连...", name, err)
		}
		ch.Close()
		time.Sleep(5 * time.Second)
	}
}

func main() {
	// 加载 .env（本地开发）
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found; continuing")
	}
	// 加载配置
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.yaml"
	}
	log.Printf("Loading config from %s", configPath)
	cfg, usedDefault, err := config.LoadLocalDev(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if usedDefault {
		log.Printf("Config File %s not found, using default local config", configPath)
	} else {
		log.Printf("Config loaded from file: %s", configPath)
	}
	// 连接数据库（带重试）
	var sqlDB *gorm.DB
	connectWithRetry("MySQL", 10, func() error {
		var err error
		sqlDB, err = db.NewDB(cfg.Database)
		return err
	})
	defer db.CloseDB(sqlDB)
	// 这两张表是本次新增的 Worker 侧依赖（死信台账、点赞事件台账），
	// 单独迁移一次，避免「API 还没重启、Worker 先上线」时死信消费者直接报错。
	// 其余业务表的建表仍由 API 进程的 AutoMigrate 负责。
	if err := db.AutoMigrateWorker(sqlDB); err != nil {
		log.Fatalf("Failed to auto migrate worker tables: %v", err)
	}

	// 连接 Redis（用于流行度更新）
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Redis config error (popularity worker disabled): %v", err)
		cache = nil
	} else {
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (popularity worker disabled): %v", err)
			_ = cache.Close()
			cache = nil
		} else {
			defer cache.Close()
			log.Printf("Redis connected (popularity worker enabled)")
		}
	}
	// 连接 RabbitMQ（带重试）
	var rmq *mqrabbit.RabbitMQ
	connectWithRetry("RabbitMQ", 10, func() error {
		var err error
		rmq, err = mqrabbit.NewRabbitMQ(&cfg.RabbitMQ)
		return err
	})
	conn := rmq.Conn
	defer rmq.Close()

	// 声明拓扑：主队列 + 专属死信队列 + 延迟重试队列（关闭后重连的临时 Channel）
	topoCh, err := rmq.NewChannel()
	if err != nil {
		log.Fatalf("Failed to open topology channel: %v", err)
	}
	for _, t := range topics {
		if t == mqrabbit.PopularityTopic && cache == nil {
			// Redis 不可用时热度 Worker 不会启动，也没必要声明它的队列。
			continue
		}
		if err := t.Declare(topoCh, mqrabbit.DefaultRetryTTL); err != nil {
			log.Fatalf("Failed to declare %s topology: %v", t.Name, err)
		}
	}
	topoCh.Close()

	// 准备 repo
	socialRepo := social.NewSocialRepository(sqlDB)
	likeRepo := video.NewLikeRepository(sqlDB)
	commentRepo := video.NewCommentRepository(sqlDB)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 队列积压 gauge：消费速率掉下去时，先看到的是 queue_depth 抬头。
	go observability.SampleQueueDepth(ctx, conn, []string{
		mqrabbit.LikeTopic.Queue,
		mqrabbit.CommentTopic.Queue,
		mqrabbit.SocialTopic.Queue,
		mqrabbit.PopularityTopic.Queue,
		mqrabbit.RetryQueueName(mqrabbit.LikeTopic.Queue),
		mqrabbit.RetryQueueName(mqrabbit.CommentTopic.Queue),
		mqrabbit.RetryQueueName(mqrabbit.SocialTopic.Queue),
		mqrabbit.RetryQueueName(mqrabbit.PopularityTopic.Queue),
	}, 15*time.Second)

	// like_events 只是去重账本，按保留期清理，避免无限增长。
	go purgeLikeEvents(ctx, likeRepo)

	pprofServer, err := observability.NewPprofServer(
		"Worker",
		cfg.ObservabilityConfig.Pprof.Enabled,
		cfg.ObservabilityConfig.Pprof.WorkerAddr,
	)
	if err != nil {
		log.Printf("Failed to start worker pprof server: %v", err)
	}
	if pprofServer != nil {
		defer pprofServer.Close()
	}

	// 每类事件一个 RetryRouter：进程内重试耗尽后由它把消息投递到重试队列，
	// 由 RabbitMQ 的 TTL 完成延迟重投。
	newRouter := func(t mqrabbit.Topic) worker.RetryPublisher {
		r, err := mqrabbit.NewRetryRouter(rmq, t.Exchange, t.Queue, mqrabbit.DefaultRetryTTL)
		if err != nil {
			log.Printf("Failed to create retry router for %s (退化为直接进入死信): %v", t.Name, err)
			return nil
		}
		return r
	}
	likeRetry := newRouter(mqrabbit.LikeTopic)
	commentRetry := newRouter(mqrabbit.CommentTopic)
	socialRetry := newRouter(mqrabbit.SocialTopic)
	popularityRetry := newRouter(mqrabbit.PopularityTopic)

	// 每个 Worker 独立 Channel + 自动重连
	go runWorkerWithRetry(ctx, "LikeWorker", conn, func(ch *amqp.Channel) error {
		return worker.NewLikeWorker(likeRepo, mqrabbit.LikeTopic.Queue, likeRetry).Run(ctx, ch)
	})
	go runWorkerWithRetry(ctx, "CommentWorker", conn, func(ch *amqp.Channel) error {
		return worker.NewCommentWorker(commentRepo, mqrabbit.CommentTopic.Queue, commentRetry).Run(ctx, ch)
	})
	go runWorkerWithRetry(ctx, "SocialWorker", conn, func(ch *amqp.Channel) error {
		return worker.NewSocialWorker(socialRepo, mqrabbit.SocialTopic.Queue, socialRetry).Run(ctx, ch)
	})
	if cache != nil {
		go runWorkerWithRetry(ctx, "PopularityWorker", conn, func(ch *amqp.Channel) error {
			return worker.NewPopularityWorker(cache, mqrabbit.PopularityTopic.Queue, popularityRetry).Run(ctx, ch)
		})
	}

	// 死信队列消费者：把「重试到上限仍然失败」的消息暴露出来（生产环境应改成告警）。
	for _, t := range topics {
		t := t
		if t == mqrabbit.PopularityTopic && cache == nil {
			continue
		}
		go runWorkerWithRetry(ctx, "DLQWorker-"+t.Name, conn, func(ch *amqp.Channel) error {
			return worker.NewDLQWorker(sqlDB, t.Queue).Run(ctx, ch)
		})
	}

	// 等待退出信号
	<-ctx.Done()
	log.Printf("Worker shutting down...")
	// 在途消息最长还要经历 1s+2s+4s 的进程内退避重试，给它留出结束的时间；
	// 没跑完的消息会在 ctx 取消时被 Nack(requeue=true) 退回队列，不会丢。
	time.Sleep(10 * time.Second)
	log.Printf("Worker stopped")
}

// purgeLikeEvents 周期性删除过期的 like 去重记录。
// 保留期远大于消息最长存活时间（重试队列几轮 TTL + 死信滞留），
// 因此不会出现「账本已删、旧消息又被重投」导致重复计数。
func purgeLikeEvents(ctx context.Context, repo *video.LikeRepository) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := repo.PurgeLikeEvents(ctx, time.Now().Add(-video.LikeEventRetention))
			if err != nil {
				log.Printf("like_events 清理失败: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("like_events 清理 %d 条过期记录", n)
			}
		}
	}
}
