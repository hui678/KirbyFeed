package http

import (
	"context"
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/feed"
	"feedsystem_video_go/internal/message"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/middleware/ratelimit"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/storage"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetRouter(db *gorm.DB, cache *rediscache.Client, rmq *rabbitmq.RabbitMQ, adminToken string, store storage.Storage) *gin.Engine {
	r := gin.Default()
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	r.Static("/static", "./.run/uploads")
	// rate_limit
	loginLimiter := ratelimit.Limit(cache, "account_login", 10, time.Minute, ratelimit.KeyByIP)
	registerLimiter := ratelimit.Limit(cache, "account_register", 5, time.Hour, ratelimit.KeyByIP)
	// 刷新接口无鉴权头，只能按 IP 限流：防止拿泄露的 refresh 反复试探/刷 access。
	refreshLimiter := ratelimit.Limit(cache, "account_refresh", 30, time.Minute, ratelimit.KeyByIP)

	likeLimiter := ratelimit.Limit(cache, "like_write", 30, time.Minute, ratelimit.KeyByAccount)
	commentLimiter := ratelimit.Limit(cache, "comment_write", 10, time.Minute, ratelimit.KeyByAccount)
	socialLimiter := ratelimit.Limit(cache, "social_write", 20, time.Minute, ratelimit.KeyByAccount)
	// 改密按账号限流：即使已经要求登录态，也要挡住对旧密码的反复尝试
	changePwdLimiter := ratelimit.Limit(cache, "account_change_password", 5, time.Hour, ratelimit.KeyByAccount)

	// account
	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache)
	accountHandler := account.NewAccountHandler(accountService, store)
	accountGroup := r.Group("/account")
	{
		accountGroup.POST("/register", registerLimiter, accountHandler.CreateAccount)
		accountGroup.POST("/login", loginLimiter, accountHandler.Login)
		accountGroup.POST("/findByID", accountHandler.FindByID)
		accountGroup.POST("/findByUsername", accountHandler.FindByUsername)
		accountGroup.POST("/refresh", refreshLimiter, accountHandler.Refresh)
	}
	protectedAccountGroup := accountGroup.Group("")
	protectedAccountGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedAccountGroup.POST("/logout", accountHandler.Logout)
		// 改密必须在登录态下调用（旧实现放在匿名组，任何人知道用户名就能试旧密码）
		protectedAccountGroup.POST("/changePassword", changePwdLimiter, accountHandler.ChangePassword)
		protectedAccountGroup.POST("/rename", accountHandler.Rename)
		protectedAccountGroup.POST("/uploadAvatar", accountHandler.UploadAvatar)
		protectedAccountGroup.POST("/updateProfile", accountHandler.UpdateProfile)
	}
	// video
	videoRepository := video.NewVideoRepository(db)
	outboxRepository := video.NewOutboxRepository(db)
	// Outbox 运维入口：failed 是终态，必须能查、能重放；否则「失败」等于静默丢弃。
	// 只有配置了 ADMIN_TOKEN 才注册，默认不暴露。
	if adminToken != "" {
		outboxAdmin := newOutboxAdminHandler(outboxRepository)
		adminGroup := r.Group("/admin/outbox", adminTokenAuth(adminToken))
		adminGroup.GET("/stats", outboxAdmin.Stats)
		adminGroup.GET("/failed", outboxAdmin.ListFailed)
		adminGroup.POST("/replay", outboxAdmin.Replay)
		log.Printf("Outbox admin API enabled: /admin/outbox/* (requires X-Admin-Token)")
	}
	popularityMQ, err := rabbitmq.NewPopularityMQ(rmq)
	if err != nil {
		log.Printf("PopularityMQ init failed (mq disabled): %v", err)
		popularityMQ = nil
	}
	videoService := video.NewVideoService(videoRepository, cache, popularityMQ)
	videoHandler := video.NewVideoHandler(videoService, accountService, store)
	chunkHandler := video.NewChunkUploadHandler(cache, store)
	videoGroup := r.Group("/video")
	{
		videoGroup.POST("/listByAuthorID", videoHandler.ListByAuthorID)
		videoGroup.POST("/getDetail", videoHandler.GetDetail)
	}
	protectedVideoGroup := videoGroup.Group("")
	protectedVideoGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedVideoGroup.POST("/uploadVideo", videoHandler.UploadVideo)
		protectedVideoGroup.POST("/uploadCover", videoHandler.UploadCover)
		protectedVideoGroup.POST("/publish", videoHandler.PublishVideo)
		protectedVideoGroup.POST("/chunk/init", chunkHandler.InitChunkUpload)
		protectedVideoGroup.POST("/chunk/upload", chunkHandler.UploadChunk)
		protectedVideoGroup.POST("/chunk/status", chunkHandler.ChunkStatus)
		protectedVideoGroup.POST("/chunk/complete", chunkHandler.CompleteChunkUpload)
	}
	// like
	likeMQ, err := rabbitmq.NewLikeMQ(rmq)
	if err != nil {
		log.Printf("LikeMQ init failed (mq disabled): %v", err)
		likeMQ = nil
	}
	likeRepository := video.NewLikeRepository(db)
	likeService := video.NewLikeService(likeRepository, videoRepository, cache, likeMQ, popularityMQ)
	likeHandler := video.NewLikeHandler(likeService)
	likeGroup := r.Group("/like")
	protectedLikeGroup := likeGroup.Group("")
	protectedLikeGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedLikeGroup.POST("/like", likeLimiter, likeHandler.Like)
		protectedLikeGroup.POST("/unlike", likeLimiter, likeHandler.Unlike)
		protectedLikeGroup.POST("/isLiked", likeHandler.IsLiked)
		protectedLikeGroup.POST("/listMyLikedVideos", likeHandler.ListMyLikedVideos)
	}
	// comment
	commentRepository := video.NewCommentRepository(db)
	commentMQ, err := rabbitmq.NewCommentMQ(rmq)
	if err != nil {
		log.Printf("CommentMQ init failed (mq disabled): %v", err)
		commentMQ = nil
	}
	commentService := video.NewCommentService(commentRepository, videoRepository, cache, commentMQ, popularityMQ)
	commentHandler := video.NewCommentHandler(commentService, accountService)
	commentGroup := r.Group("/comment")
	{
		commentGroup.POST("/listAll", commentHandler.GetAllComments)
	}
	protectedCommentGroup := commentGroup.Group("")
	protectedCommentGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedCommentGroup.POST("/publish", commentLimiter, commentHandler.PublishComment)
		protectedCommentGroup.POST("/delete", commentLimiter, commentHandler.DeleteComment)
	}
	// social
	socialMQ, err := rabbitmq.NewSocialMQ(rmq)
	if err != nil {
		log.Printf("SocialMQ init failed (mq disabled): %v", err)
		socialMQ = nil
	}
	socialRepository := social.NewSocialRepository(db)
	socialService := social.NewSocialService(socialRepository, accountRepository, socialMQ, cache)
	socialHandler := social.NewSocialHandler(socialService)
	socialGroup := r.Group("/social")
	protectedSocialGroup := socialGroup.Group("")
	protectedSocialGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedSocialGroup.POST("/follow", socialLimiter, socialHandler.Follow)
		protectedSocialGroup.POST("/unfollow", socialLimiter, socialHandler.Unfollow)
		protectedSocialGroup.POST("/getAllFollowers", socialHandler.GetAllFollowers)
		protectedSocialGroup.POST("/getAllVloggers", socialHandler.GetAllVloggers)
		protectedSocialGroup.POST("/getCounts", socialHandler.GetCounts)
	}

	accountGroup.POST("/getProfile", func(c *gin.Context) {
		var req account.GetProfileRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if req.AccountID == 0 {
			c.JSON(400, gin.H{"error": "account_id is required"})
			return
		}
		acc, err := accountService.FindByID(c.Request.Context(), req.AccountID)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		videoCount, _ := videoRepository.CountByAuthor(c.Request.Context(), req.AccountID)
		totalLikes, _ := videoRepository.TotalLikesByAuthor(c.Request.Context(), req.AccountID)
		followerCount, _ := socialRepository.CountFollowers(c.Request.Context(), req.AccountID)
		vloggerCount, _ := socialRepository.CountVloggers(c.Request.Context(), req.AccountID)

		// 账号头像在库里是 object key，响应时解析成可访问 URL（预签名 / 本地静态路径）。
		avatarURL := acc.AvatarURL
		if u, rErr := store.Resolve(c.Request.Context(), acc.AvatarURL); rErr == nil {
			avatarURL = u
		}

		c.JSON(200, account.GetProfileResponse{
			Account:    account.FindByIDResponse{ID: acc.ID, Username: acc.Username, AvatarURL: avatarURL, Bio: acc.Bio},
			VideoCount: videoCount, TotalLikes: totalLikes,
			FollowerCount: followerCount, VloggerCount: vloggerCount,
		})
	})
	// feed
	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, likeRepository, cache, store)
	feedHandler := feed.NewFeedHandler(feedService)
	// 热榜快照后台预生成：把「合并 60 个分钟桶」这个 O(N) 阻塞命令从读请求路径上挪走。
	feed.StartHotSnapshotBuilder(context.Background(), feedService, time.Minute)
	// 部分丢失自愈：只判断 ZSET 是否为空的旧逻辑发现不了「少了几条」
	feed.StartTimelineReconciler(context.Background(), feedService, feed.TimelineReconcileInterval)
	feedGroup := r.Group("/feed")
	feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))
	{
		feedGroup.POST("/listLatest", feedHandler.ListLatest)
		feedGroup.POST("/listByPopularity", feedHandler.ListByPopularity)
		feedGroup.POST("/listTags", feedHandler.ListTags)
		feedGroup.POST("/listByTag", feedHandler.ListByTag)
	}
	protectedFeedGroup := feedGroup.Group("")
	protectedFeedGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedFeedGroup.POST("/listByFollowing", feedHandler.ListByFollowing)
	}
	// message
	messageRepo := message.NewRepository(db)
	messageService := message.NewService(messageRepo)
	messageHandler := message.NewHandler(messageService)
	messageGroup := r.Group("/message")
	protectedMessageGroup := messageGroup.Group("")
	protectedMessageGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedMessageGroup.POST("/send", messageHandler.Send)
		protectedMessageGroup.POST("/list", messageHandler.List)
	}
	//worker：Outbox 投递 + 时间线消费。
	// 两者都自带重连/自愈，MQ 未就绪时不再永久退出，恢复后自动补发积压消息。
	workerCtx := context.Background()

	// 时间线队列同样需要重试队列 + 死信队列：Outbox 保证的是"至少一次投递"，
	// 如果消费端失败后直接 Ack 丢弃，前面所有可靠性设计都在最后一环作废。
	var timelineRetry worker.RetryPublisher
	if rmq != nil {
		if tlCh, err := rmq.NewChannel(); err == nil {
			if err := rabbitmq.TimelineTopic.Declare(tlCh, rabbitmq.DefaultRetryTTL); err != nil {
				log.Printf("timeline topology init failed: %v", err)
			}
			tlCh.Close()
		}
		router, err := rabbitmq.NewRetryRouter(rmq,
			rabbitmq.TimelineTopic.Exchange, rabbitmq.TimelineTopic.Queue, rabbitmq.DefaultRetryTTL)
		if err != nil {
			log.Printf("timeline retry router init failed: %v", err)
		} else {
			timelineRetry = router
		}
	}

	worker.StartOutboxPoller(workerCtx, outboxRepository, rmq)
	worker.StartConsumer(workerCtx, rmq, rabbitmq.TimelineTopic.Queue, cache, timelineRetry)

	// 死信队列也要有人消费，否则它只是把失败消息挪了个地方、仍然没人知道。
	// 这几个队列的消费者都在 API 进程内，所以它们的死信消费者也在这里启动。
	startDLQConsumers(workerCtx, db, rmq, []string{
		rabbitmq.TimelineTopic.Queue,
		"notification.like",
		"notification.comment",
		"notification.social",
	})

	// 这四类队列的消费者都在 API 进程内，积压指标也在这里采样。
	if rmq != nil {
		queues := []string{
			rabbitmq.TimelineTopic.Queue,
			rabbitmq.RetryQueueName(rabbitmq.TimelineTopic.Queue),
			"notification.like", "notification.comment", "notification.social",
			rabbitmq.RetryQueueName("notification.like"),
			rabbitmq.RetryQueueName("notification.comment"),
			rabbitmq.RetryQueueName("notification.social"),
		}
		go observability.SampleQueueDepth(workerCtx, rmq.Conn, queues, 15*time.Second)
	}

	// SSE notification
	// 通知队列与业务队列绑定在**同一个交换机**上：一次事件、多份消费（落库一份、通知一份）。
	// 因此它们同样需要各自的重试队列和死信队列，否则通知处理失败会被静默丢弃。
	notifTopics := []struct {
		exchange string
		queue    string
		binding  string
	}{
		{"like.events", "notification.like", "like.like"},
		{"comment.events", "notification.comment", "comment.publish"},
		{"social.events", "notification.social", "social.follow"},
	}
	notifRetry := map[string]worker.RetryPublisher{}
	if rmq != nil {
		if notifCh, err := rmq.NewChannel(); err == nil {
			for _, t := range notifTopics {
				if err := rabbitmq.DeclareTopic(notifCh, t.exchange, t.queue, t.binding); err != nil {
					log.Printf("notification topic init failed (%s): %v", t.queue, err)
					continue
				}
				if err := rabbitmq.DeclareRetry(notifCh, t.exchange, t.queue, rabbitmq.DefaultRetryTTL); err != nil {
					log.Printf("notification retry topology init failed (%s): %v", t.queue, err)
				}
			}
			notifCh.Close()
		}
		for _, t := range notifTopics {
			router, err := rabbitmq.NewRetryRouter(rmq, t.exchange, t.queue, rabbitmq.DefaultRetryTTL)
			if err != nil {
				log.Printf("notification retry router init failed (%s): %v", t.queue, err)
				continue
			}
			notifRetry[t.queue] = router
		}
	}
	sseHub := worker.NewSSEHub(db, cache, accountRepository)
	notifGroup := r.Group("/notification")
	notifGroup.Use(sseHub.SSERequireAuth())
	sseHub.RegisterRoutes(r, notifGroup)

	go func() {
		if rmq != nil {
			hub := sseHub
			ctx := context.Background()
			// 每个 notification worker 独立 Channel + 自动重连
			for _, q := range []string{"notification.like", "notification.comment", "notification.social"} {
				go func(queue string) {
					for {
						ch, err := rmq.NewChannel()
						if err != nil {
							log.Printf("notification-%s: 创建 Channel 失败: %v, 5秒后重试", queue, err)
							time.Sleep(5 * time.Second)
							continue
						}
						if err := ch.Qos(50, 0, false); err != nil {
							log.Printf("notification-%s: QoS 设置失败: %v", queue, err)
						}
						w := worker.NewNotificationWorker(db, queue, hub, notifRetry[queue])
						if err := w.Run(ctx, ch); err != nil {
							log.Printf("notification-%s: %v, 5秒后重连...", queue, err)
						}
						ch.Close()
						time.Sleep(5 * time.Second)
					}
				}(q)
			}
		} else {
			log.Printf("Notification SSE disabled (MQ not available)")
		}
	}()

	return r
}

// startDLQConsumers 为「在 API 进程内消费」的队列启动死信消费者。
//
// 死信队列只做「记录 + 告警」：消息落进 dead_letters 表并计数，不重新投递。
// 能走到这里的消息已经经历了进程内退避重试和多轮 TTL 延迟重投，继续 requeue
// 只会让一条注定失败的消息反复冲击数据库。
func startDLQConsumers(ctx context.Context, db *gorm.DB, rmq *rabbitmq.RabbitMQ, mainQueues []string) {
	if rmq == nil {
		return
	}
	for _, q := range mainQueues {
		go func(mainQueue string) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				ch, err := rmq.NewChannel()
				if err != nil {
					log.Printf("dlq-%s: 创建 Channel 失败: %v, 5秒后重试", mainQueue, err)
					time.Sleep(5 * time.Second)
					continue
				}
				if err := worker.NewDLQWorker(db, mainQueue).Run(ctx, ch); err != nil {
					if ctx.Err() != nil {
						ch.Close()
						return
					}
					log.Printf("dlq-%s: %v, 5秒后重连...", mainQueue, err)
				}
				ch.Close()
				time.Sleep(5 * time.Second)
			}
		}(q)
	}
}
