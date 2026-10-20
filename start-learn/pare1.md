这里为啥要设置路由啊

路由不应该是对应url和对应的处理函数吗？

这里启动了数据库，Redis，rmq和路由有啥关系？

还是说我们开发时写的路由表，这里是创建的路由实例

19:16

TRAE

任务耗时 36s

参考内容

思考过程

你的最后一个猜测是对的： 这里是“创建路由实例并注册路由表” 。但你把“路由”理解成“一张 URL→函数的表”，这个理解只对了一半，缺的那一半正好解释了为什么它要接收 db/cache/rmq。

## 先拆开这一行到底做了什么
`SetRouter` 内部实际上干了 三件事 ，不是一件事：

Go

1
2
3
4
5
6

`func SetRouter(db, cache, rmq) *gin.Engine {
    r := gin.Default()        // ① 创建一个空的 gin.Engine 实例
    ...                       // ② 装配所有依赖对象（repo → service → handler）
    r.POST("/account/login", accountHandler.Login)  // ③ 往实例上注册路由
    return r
}`

- ①`gin.Default()` 才是你说的“路由 实例 ”——一个空对象，此时一条路由都没有。
- ③ 你平时说的“路由表”（URL 对应处理函数），就是靠一条条`r.POST(path, handler)` 注册到这个实例上 长出来的。所以“路由表”不是一份静态文件，而是在实例上调 API 调出来的。
- ② 就是这一块，是重点，也是你觉得“跟数据库有啥关系”的地方。
## 为什么“注册路由”必须先有 db/redis/rmq
关键点： Gin 要求你传进去的第二个参数是一个真实的函数 ，而这里的函数不是“裸函数”，是 绑了依赖的方法 。

看`account` 这一条链（ router.go ）：

Go

1
2
3
4

`accountRepository := account.NewAccountRepository(db)          // 要 db
accountService    := account.NewAccountService(repository, cache) // 要 repo + cache
accountHandler    := account.NewAccountHandler(service)         // 要 service
r.POST("/account/login", accountHandler.Login)                  // 这里才注册路由`

`accountHandler.Login` 这个“处理函数”，它内部真的会去读数据库、查 Redis 做限流/鉴权。所以：

- 没有`db` → 造不出 repository → 造不出 service → 造不出 handler → `accountHandler.Login` 根本不存在，你没法注册这条路由 。
- 同理`cache` 给了限流中间件和 JWT，`rmq` 给了点赞/评论等 MQ 封装。
也就是说： db/cache/rmq 不是“和路由并列的另一个步骤”，而是路由的原材料。 先有依赖，才能造 handler，才能注册 URL。这三步是被迫按顺序串起来的。

## 那为什么要把这些全塞进一个函数
这是 Gin 的 惯例 ，也是这个项目刻意的写法：Gin 本身并不管依赖注入，它只要你给 handler 函数。谁负责造 handler？没人管。所以作者把**“依赖装配”和“路由注册”合并进`SetRouter` 一个函数**——因为造 handler 和注册路由本来就无法分开。

也能拆成两段（先`NewXxxHandler` 造好，再单独注册），但那样 handler 对象得存到某处传来传去，反而更啰嗦。

## 为什么 main.go 里必须调它
main.go 的角色是 纯启动器 ：连数据库 → 连 Redis → 连 MQ → 起 pprof，只有一个目的—— 把这些准备好的依赖交出去 。交给谁？就是交给`SetRouter` 。

`SetRouter` 返回一个配置好的`gin.Engine` （也就是带路由表的“路由实例”），接着：

Go

1

`r.Run(":" + port)   // 让这个实例开始监听端口`

## 小结（直接回答你的四个问号）
你的问题 答案 这里为啥要设置路由 因为 main 只负责“备好依赖”，真正造路由实例、注册路由表的是`SetRouter` ，main 调它拿到实例才能`Run` 起监听 路由不就是 URL→处理函数吗 对，但处理函数是 绑了依赖的方法 （如`accountHandler.Login` ），不是裸函数 db/Redis/rmq 跟路由啥关系 它们是 原材料 ：db → repo、cache → service/限流、rmq → MQ 封装，一路造出 handler，才能注册路由 是路由表还是路由实例 `gin.Default()` 造 实例 ；`r.POST(...)` 往实例上注册的就是你说的 路由表 。你在 router.go 里写的那一大堆`r.POST` 就是那张表

一句话： 路由表的内容在`router.go` 里写，但“表”要挂在`SetRouter` 造出来的实例上；而注册表之前必须先用手里的 db/cache/rmq 把每个 handler 造出来。 所以这一行既是“创建路由实例”，也顺带完成了整张表的注册和全项目的
啥意思啥叫拓扑，没太懂，我能知道这四个是干嘛的

19:30

TRAE

思考过程

思考

“拓扑”这个词太书面了，说白了就是 先把房间造好，再开始收发消息 。

## 用快递站类比
把 RabbitMQ 想象成一个快递中转站。消息就是一个个包裹。在能收发包裹之前，你得先把这个中转站的基础设施建好：

- 交换机（Exchange） = 中转站的 分拣中心 。所有包裹先送到这里，它根据“目的地标签（routing key）”决定往哪个方向发。
- 队列（Queue） = 一个个 货架 。包裹最终被放到货架上，等着 worker 来取。
- 绑定（Binding） = 从分拣中心到货架的 传送带规则 ，规定“带什么标签的包裹送到哪个货架”。
1

`包裹(消息) ──► 分拣中心(Exchange) ──按routing key分流──► 货架(Queue) ──► worker取走处理`

这三样东西（交换机 + 队列 + 传送带规则）合起来，就叫 拓扑（topology） 。所谓“声明拓扑”，就是一次性把这些东西建好。

## 为什么要先声明？
RabbitMQ 不允许你往 不存在的交换机 发消息，也不允许你从 不存在的队列 取消息。所以程序启动第一件事就是：

Go

1
2
3

`ch.ExchangeDeclare(...)   // 建分拣中心
ch.QueueDeclare(...)      // 建货架
ch.QueueBind(...)         // 连传送带`

幂等 的意思是：这些操作重复调用不会出错。第一次调用是“新建”，第二次调用发现“已经存在了”就直接跳过（前提是参数一致）。所以 API 进程和 worker 进程都会各自声明一遍，谁先启动都不会冲突。

## 回到代码：这 4 组拓扑分别装什么消息
代码 worker/main.go 顶部定义了 4 组，每组 3 个常量（交换机名、队列名、绑定规则）：

组 交换机（分拣中心） 队列（货架） 装什么消息 谁来取走处理 ① `social.events` `social.events` 谁关注了谁 / 谁取关了谁 SocialWorker ② `like.events` `like.events` 谁点赞了哪个视频 / 取消点赞 LikeWorker ③ `comment.events` `comment.events` 谁发表了评论 CommentWorker ④ `video.popularity.events` `video.popularity.events` 某个视频热度 +1 或 -1 PopularityWorker

第 ④ 组比较特殊：它只在 Redis 可用时才声明（ worker/main.go ），因为热度统计最终写 Redis。

## 举一个完整的例子：点赞
1
2
3
4
5
6
7
8
9

`1. 用户点了个赞
   ↓ API 进程
2. likeService 调用 likeMQ.Like() 发一条消息到 like.events 交换机
   ↓ 交换机按 routing key "like.like" 分流
3. 消息落到 like.events 队列
   ↓
4. worker 进程的 LikeWorker goroutine 从队列取走消息
   ↓
5. 更新数据库的点赞计数`

这就是为什么要拆出 4 组： 4 类业务消息各走各的通道，互不干扰 ，哪类消息积压了就只扩对应的 worker。

## 还有一个细节：死信交换机
每个队列声明时都挂了一个参数：

Go

1

`amqp.Table{"x-dead-letter-exchange": mqrabbit.DLXExchange}`

这叫 死信交换机（DLX） 。意思是：如果一条消息被拒绝（Nack）或者过期没人取，就自动转到这个“回收站”交换机，而不是直接扔掉。方便事后排查丢了哪些消息。

一句话总结： 拓扑 = 提前把 4 个分拣中心 + 4 个货架 + 传送带规则建好，然后 API 往里放消息，worker 从货架取消息处理。 4 组分别对应关注、点赞、评论、热度这 4 类异步事件。
这里内阁模块对应的方法名好像都不太一样，但是内容好像是一样的，都是三层，这是咋做的

另外这是干嘛的我没懂，你举一个路由的例子我看看

19:44

TRAE

任务耗时 46s

参考内容

好问题，这两个点一起看。我先读一下 account 模块的三个文件，用真实代码给你讲。

已读取 3 个文件，搜索 1 次文件

看完真实代码了。你选中的这三行，就是 Go 后端最经典的 三层架构 。我一个一个拆开讲。

## 一、这三层各是什么
### 第 1 层：Repository（数据层）—— 只管读写数据库
对应 repo.go ：

Go

1
2
3
4
5
6
7

`type AccountRepository struct {
    db *gorm.DB   // 它手里只有一样东西：数据库连接
}

func NewAccountRepository(db *gorm.DB) *AccountRepository {
    return &AccountRepository{db: db}
}`

它的方法全是 纯粹的 SQL 操作 ，没有任何业务判断。比如 repo.go#L58-L64 ：

Go

1
2
3
4
5
6
7

`func (ar *AccountRepository) FindByID(ctx context.Context, id uint) (*Account, error) {
    var account Account
    if err := ar.db.WithContext(ctx).First(&account, id).Error; err != nil {
        return nil, err
    }
    return &account, nil
}`

就是"按 id 查一条记录"。它不知道这个查询是用来登录还是查资料， 它只负责执行 SQL 。

这一层能用的工具 ：`db` （MySQL 连接）。别的什么都没有。

### 第 2 层：Service（业务层）—— 只管业务规则
对应 service.go ：

Go

1
2
3
4
5
6
7
8

`type AccountService struct {
    accountRepository *AccountRepository   // 手里有数据层
    cache             *rediscache.Client   // 还有 Redis 缓存
}

func NewAccountService(accountRepository *AccountRepository, cache *rediscache.Client) *AccountService {
    return &AccountService{accountRepository: accountRepository, cache: cache}
}`

它的方法是 业务逻辑 。比如注册账号 service.go#L33-L43 ：

Go

1
2
3
4
5
6
7
8
9
10
11
12
13

`func (as *AccountService) CreateAccount(ctx context.Context, account *Account) error {
    // 业务规则1：密码必须加密存储，不能存明文
    passwordHash, err := bcrypt.GenerateFromPassword([]byte(account.Password), bcrypt.DefaultCost)
    if err != nil {
        return err
    }
    account.Password = string(passwordHash)
    // 业务规则2：加密完之后，调数据层真正写库
    if err := as.accountRepository.CreateAccount(ctx, account); err != nil {
        return err
    }
    return nil
}`

注意看： 它自己不写 SQL 。它做"密码加密"这个业务动作，做完之后调`as.accountRepository.CreateAccount(...)` 让数据层去落库。

再看一个改名的例子 service.go#L45-L74 ，业务规则更多：新用户名不能为空 → 生成新 token → 调 repo 同时改用户名和 token（事务）→ 把 token 写进 Redis 缓存。 这些"先做什么后做什么"的编排，全在 Service 层 。

这一层能用的工具 ：`accountRepository` （要读写数据就找它）、`cache` （要缓存就找它）。 它手里没有`db` ，写不了 SQL 。

### 第 3 层：Handler（HTTP 层）—— 只管 HTTP 请求的进出
对应 handler.go ：

Go

1
2
3
4
5
6
7

`type AccountHandler struct {
    accountService *AccountService   // 手里有业务层
}

func NewAccountHandler(accountService *AccountService) *AccountHandler {
    return &AccountHandler{accountService: accountService}
}`

它的方法是 gin 的处理函数 ，签名固定是`func(c *gin.Context)` 。比如注册 handler.go#L28-L42 ：

Go

1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18

`func (h *AccountHandler) CreateAccount(c *gin.Context) {
    var req CreateAccountRequest
    // 第1步：把 HTTP 请求的 JSON body 解析成 Go 结构体
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
        return
    }
    // 第2步：调业务层干活
    if err := h.accountService.CreateAccount(c.Request.Context(), &Account{
        Username: req.Username,
        Password: req.Password,
    }); err != nil {
        c.JSON(500, gin.H{"error": err.Error()})
        return
    }
    // 第3步：把结果转成 HTTP 响应返回
    c.JSON(200, gin.H{"message": "account created"})
}`

它干的事就三件： 解析请求参数 → 调 Service → 返回 JSON 。它不知道密码怎么加密，也不知道数据存哪张表。

这一层能用的工具 ：`accountService` 。 它手里既没有`db` 也没有`cache` ，碰不到数据库 。

## 二、回到 router.go 那三行，现在再看
Go

1
2
3

`accountRepository := account.NewAccountRepository(db)                    // 第1层：给它 db
accountService    := account.NewAccountService(accountRepository, cache) // 第2层：给它 repo + cache
accountHandler    := account.NewAccountHandler(accountService)           // 第3层：给它 service`

这是一个 依赖注入链条 ，像递工具一样：

- 造 Repository 时，把`db` 递进去 → Repository 从此能操作数据库
- 造 Service 时，把`repository` 和`cache` 递进去 → Service 从此能读写数据、用缓存
- 造 Handler 时，把`service` 递进去 → Handler 从此能调用业务逻辑
注意方向是单向的 ：Handler 能调 Service，Service 能调 Repository，但反过来不行。Repository 完全不知道有 Handler 存在。

## 三、一个路由的完整例子（你要求的）
就拿`POST /account/register` 这条，从 router.go#L45 开始：

Go

1

`accountGroup.POST("/register", registerLimiter, accountHandler.CreateAccount)`

这行的意思是： 当收到`POST /account/register` 请求时，先过`registerLimiter` （限流中间件），然后执行`accountHandler.CreateAccount` 这个函数 。

现在模拟一个真实请求，前端发来：

1
2
3
4

`POST /account/register
Content-Type: application/json

{"username": "zhangsan", "password": "123456"}`

完整流程，每一行对应代码里的哪一层：

1
2
3
4
5
6
7
8
9
10
11
12
13
14
15

`① gin 收到请求，匹配到 /account/register
② 过 registerLimiter 中间件：查 Redis，这个 IP 一小时内注册几次了？超 5 次直接拒掉
③ 进入 Handler 层：accountHandler.CreateAccount(c)
     handler.go:30  c.ShouldBindJSON(&req)
                    → req.Username = "zhangsan", req.Password = "123456"
④ Handler 调 Service 层：h.accountService.CreateAccount(ctx, &Account{...})
     service.go:34  bcrypt.GenerateFromPassword("123456")
                    → 密码变成哈希串 "$2a$10$xxx..."
     service.go:39  as.accountRepository.CreateAccount(ctx, account)
⑤ Service 调 Repository 层：
     repo.go:18     ar.db.Create(account)
                    → 执行 SQL：INSERT INTO accounts (username, password) VALUES ('zhangsan', '$2a$10$xxx...')
⑥ 逐层返回：repo 返回 nil（成功）→ service 返回 nil → handler 执行
     handler.go:41  c.JSON(200, {"message": "account created"})
⑦ 前端收到：HTTP 200 {"message": "account created"}`

如果中间任何一步失败，就在那一层直接`c.JSON(错误码, ...)` 返回，不再往下走。

## 四、为什么要拆三层（不拆会怎样）
如果不拆，把所有代码塞进 Handler 一个函数里，也能跑。但会有三个问题：

1. 没法复用 ：`FindByID` 这个查询，登录要用、查资料要用、鉴权中间件也要用。写在 Handler 里就得抄三遍；写在 Repository 里大家调同一个函数。
2. 没法测试 ：想测"密码加密对不对"，如果逻辑混在 Handler 里，必须启动 HTTP 服务发请求才能测。拆开后直接`service.CreateAccount(...)` 单测。
3. 改一处动全身 ：哪天要从 MySQL 换成 PostgreSQL，三层架构只用改 Repository 一个文件；混着写就得改所有 Handler。
一句话总结： Repository 管"怎么存取"，Service 管"业务上允许怎么做"，Handler 管"HTTP 请求怎么进怎么出" 。router.go 里那三行就是把这三层按依赖顺序new出来，每个模块（account/video/like/comment...）都是同样的三行套路，只是名字不同。

要不要我接着讲`registerLimiter` （限流中间件）内部是怎么实现的，或者 JWT 鉴权中间件是怎么读 token 的？

不同模块对应的很多都是这三步，为啥不直接封装个接口？而是每个模块都写三层函数

19:51

TRAE

任务耗时 40s

问到点子上了。我先确认一个事实，再回答。

已搜索 1 次文件

先看你这个观察对不对——把全部模块的构造函数签名拉出来对比：

Repository 层 （7 个模块，签名 完全一样 ）：

Go

1
2

`NewAccountRepository(db)   NewVideoRepository(db)   NewLikeRepository(db)
NewCommentRepository(db)   NewSocialRepository(db)  NewFeedRepository(db)  ...`

Service 层 （开始分化， 每个都不一样 ）：

Go

1
2
3
4
5
6
7

`NewAccountService(repo, cache)
NewVideoService(repo, cache, popularityMQ)                        // 多了个 MQ
NewLikeService(repo, videoRepo, cache, likeMQ, popularityMQ)      // 5 个依赖
NewCommentService(repo, videoRepo, cache, commentMQ, popularityMQ)
NewSocialService(repo, accountRepo, socialMQ, cache)
NewFeedService(repo, likeRepo, cache)                             // 跨模块拿了别人的 repo
NewChunkUploadHandler(cache)                                      // 只有 cache`

Handler 层 （也不一样）：

Go

1
2
3
4

`NewAccountHandler(service)
NewVideoHandler(service, accountService)     // 多依赖了一个别的 Service
NewCommentHandler(service, accountService)
NewLikeHandler(service)                      // 这个才是"标准"的`

这个对比就是答案的核心，我分四点说。

## 一、这个项目其实已经"半封装"了
你没注意到的一个细节： 所有构造函数命名都是`NewXxxRepository / NewXxxService / NewXxxHandler` 。

这本身就是 Go 社区约定俗成的"非正式接口"。Go 的哲学是 隐式接口 ——不需要提前声明`type Repository interface` ，只要大家长得一样，后面想抽象随时能抽象。现在不抽象，是因为 还没到时候 （下面讲为什么）。

## 二、三层壳子一样，填充物完全不同
你说"内容好像是一样的"，这句话只对了一半。我再拆细一点：

层 壳子（结构） 填充物（真正的代码） Repository 都是`struct{ db }` + CRUD 方法 每个模块操作的 表不同、SQL 不同 Service 都是`struct{ repo, ... }` + 业务方法 每个模块的 业务规则完全不同 Handler 都是`func(c *gin.Context)` 每个接口的 参数结构、返回格式不同

能封装的只有 壳子 ，填充物没法封装。举例：

`AccountRepository.Rename` 执行的是`UPDATE accounts SET username=?` ，而`VideoRepository.Publish` 执行的是`INSERT INTO videos ...` 。 SQL 语句、表名、字段完全不同 ，封装成接口你也还是要各写各的——只是换了个地方写。

Service 层差异更大：账号模块要 bcrypt 加密，视频模块要写 outbox 表，点赞模块要发 MQ。 业务逻辑本来就长不一样，没法封 。

所以真正能封掉的"重复代码"，其实只有那三行`New...` 赋值——这三行就算封装了，也只省 3 行代码，不值。

## 三、为什么 Go 不用 Java 那种"接口 + 泛型 BaseService"打法
这是你最可能的疑问点（"像 Java 那样封装个`BaseService<T>` 不就完了"）。三个硬原因：

原因 1：构造函数签名不统一，接口就没法定义。

看上面 Service 层的对比：有的要 2 个依赖，有的要 5 个，有的要别的模块的 Service。你写一个`NewService(deps ...any)` 的通用接口？那类型安全就丢了——到底传几个、传什么类型，编译器不再帮你检查，错了要运行时才发现。 用类型安全换省三行代码，Go 社区认为血亏。

原因 2：跨层调用走组合，不走继承。

注意 feed/service.go#L32 ：`NewFeedService(repo, likeRepo, cache)` —— Feed 的 Service 里 组合 了 video 模块的`LikeRepository` 。还有 video/video_handler.go#L26 ：`NewVideoHandler(service, accountService)` —— Video 的 Handler 组合了 account 的 Service。

如果搞一个统一的`BaseService` 接口让大家都继承，这种"我里面要放两个不同模块的零件"的灵活组合就做不到了。Go 的设计是**"需要什么依赖，就作为参数传进来"**，比"继承一个预设好依赖的基类"灵活得多。

原因 3：依赖关系必须一眼能看清。

看 router.go 这三行，依赖图是 显式 的：

Go

1
2
3

`NewAccountRepository(db)              // account 依赖 db
NewAccountService(repo, cache)        // account service 依赖 repo + cache
NewVideoHandler(service, accountSvc)  // video handler 依赖两个 service`

谁在依赖谁，不用点进函数体就知道。如果封装成一个`Register("account")` 的黑盒，依赖关系就被藏进黑盒里了，排查"为什么 video 模块会调 account 的代码"这种问题时就得多挖一层。 显式 > 隐式 ，这是 Go 的核心价值观（"Clear is better than clever"）。

## 四、什么时候才真的需要抽接口？
不是"看三行像不像"，是看 有没有多实现 。两种情况：

① 要写单元测试做 Mock。 比如想测`AccountService` 但不想连真数据库，这时才会给 Repository 抽一个接口，测试时塞个假的进去。这个项目还没抽，说明测试还没做到那一步。

② 同一层有多种实现要切换。 比如缓存可能用 Redis 也可能用 Memcached，存储可能用 MySQL 也可能用 Postgres——这时抽象出接口，调用方就感知不到下面换了。这个项目 Redis/MySQL 都是写死的，所以没必要。

一句话： 接口是给"变"准备的，不是给"像"准备的 。这三层只是"长得像"，行为上没有第二种实现要换，所以不抽。等哪天需求来了再抽，Go 的隐式接口让"事后抽接口"成本很低，不着急。

总结 ：

- 三层 骨架 相同，但每层 真正干的事 （SQL、业务规则、参数）天差地别，能封的只有那三行 new，封了也省不了什么。
- Service/Handler 的 构造函数参数本来就不一样了 （依赖数量从 1 个到 5 个不等），强行统一接口会丢类型安全。
- Go 用 组合 + 显式传参 表达依赖，比继承基类灵活，也比黑盒注册好排查。
- Go 的接口是隐式的， 等到真要 Mock 或换实现时再抽 ，才是这门语言的惯用法。现在这三行就是最经济的写法。
要不要我接着讲：如果以后要接入第二种存储（比如把视频元数据迁到 MongoDB），这个三层结构要怎么改才最省力？
那这些泡在api进程里，和worker进程，rmq的关系是什么，

为什么会用到worker

20:00

TRAE

思考过程

好问题。先纠正一个最容易混淆的点，然后逐个拆开说。

## 先厘清：RabbitMQ 和进程的关系
RabbitMQ 是个 独立的中间件服务 （和 MySQL 一样跑在别处）。它对进程没有任何"归属"概念—— 任何进程都可以连上它，既可以发消息（生产者），也可以收消息（消费者） 。

所以真实的关系是：

1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19

`                 ┌──────────────── RabbitMQ ────────────────┐
                 │  exchange: video.timeline.events          │
                 │  exchange: like.events                    │
                 │  exchange: comment.events                 │
                 │  exchange: social.events                  │
                 │  exchange: video.popularity.events        │
                 └───────▲───────────────────┬───────────────┘
              发消息 │                    │ 收消息
                     │                    │
        ┌────────────┴─────┐    ┌────────▼────────┐
        │   API 进程        │    │  Worker 进程     │
        │                   │    │                 │
        │ ① 发：点赞/评论/  │    │ 收：like/comment/│
        │    关注/热度事件   │    │   social/       │
        │ ② 发：timeline   │    │   popularity    │
        │    (outbox轮询)   │    │   4个队列       │
        │ ③ 收：timeline   │    │                 │
        │ ④ 收：notification│    │                 │
        └──────────────────┘    └─────────────────┘`

也就是说： API 进程主要是生产者，但也兼职当了两个队列的消费者 。这就是你疑惑的来源。

## 为什么 timeline 和 notification 的消费者放在 API 进程
判断"消费逻辑该放哪个进程"，核心看一条： 消费的结果给谁用？

### ① timeline 消费者 —— 结果是给"读 feed 的接口"用的
它的活只有一件事：把`videoID` 写进 Redis 的 ZSet`feed:global_timeline` （见 outboxworker.go ）。这个 ZSet 是`/feed/listLatest` 接口要读的数据。

特点：

- 干的活 极轻 （一次 Redis 写入 + 一次裁剪），没有任何数据库写
- 要求 延迟低 ——用户发了视频，刷 feed 马上要能看到
- 写 Redis 是 幂等 的（ZADD 同一个成员+分数，重复写没副作用）
### ② notification 消费者 —— 结果只能给"连着这个进程的浏览器"用
SSE 推送的前提是：浏览器和 API 进程之间有一条 长连接 ，这条连接存在 API 进程的内存里（`SSEHub` ）。Worker 进程根本没有 HTTP 端口、没有这些连接， 消息就算被 worker 收到，也推不出去 。所以通知消费 物理上必须 在 API 进程里。

### ③ outbox 轮询 —— 它是生产者不是消费者
它扫`outbox_msgs` 表，把"视频已发布"这件事从数据库搬到 MQ。放在 API 进程是因为：写 outbox 表的代码就在 API 进程里（ video_service.go ），生产者贴着业务代码放最自然。

## 为什么 like/comment/social/popularity 放 worker 进程
对照上面三条，这 4 类任务的特点完全相反：

维度 API 内的消费者（timeline/通知） Worker 的消费者（like 等 4 个） 消费结果给谁 马上要读到的缓存、在线浏览器 数据库计数、热度分， 没人急着读 单次处理重量 一次 Redis 写 MySQL 事务写 + 计数更新， 重 失败代价 丢了最多时间线少一条，可容忍 丢了点赞数就错了， 必须重试 + 死信队列 流量特征 平稳 突发（点赞风暴时队列会大量积压）

关键在"积压"这一点：点赞高峰期消息会在队列里堆起来，这时需要 加 consumer 副本去追 。如果消费者混在 API 进程里，你为了多几个点赞消费者，就得把整个 API 服务（连同所有 HTTP 路由、连接池）复制一份——又重又危险。拆出 worker 后，`docker-compose up --scale worker=5` 就解决了。

另外注意 worker/main.go 里 worker 的 QoS 是 50 （一次预取 50 条批量处理），而 outboxworker.go 里 timeline 消费者的 QoS 只有 10 ——这个差异本身就说明了设计意图：worker 追求吞吐，API 内的消费者追求低延迟、不抢资源。

## 一个诚实的补充：这个设计的前提
把消费者放进 API 进程有个隐含约束： API 进程基本只能跑单副本 。

- 如果 API 扩成 2 个副本，两个副本都会抢`notification.like` 队列，一条点赞通知只会被其中一个副本收到，但目标用户的浏览器可能连着另一个副本 → 通知丢失。
- timeline 消费倒没事（ZADD 幂等，重复消费无害），outbox 轮询也没事（发重了大不了消费端幂等去重）。
所以可以理解为： 这套架构的 API 层目前是单副本设计，水平扩展的能力全部留给了 worker 层 。要真做多副本 API，notification 队列得改成 fanout（每个实例一条独立队列），这是一个真实面试/答辩里值得讲的点。

一句话总结 ：RabbitMQ 是中立的快递站；谁发谁收只看"结果给谁用"。结果给在线用户/读接口的轻活 → 留在 API 进程顺手干；结果是落库的重活、会积压、需要独立扩容 → 交给 worker 进程。