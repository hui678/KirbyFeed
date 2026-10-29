<p align="center">
  <img src="picture/整体架构.png" alt="整体架构" width="720"/>
</p>

<h1 align="center">🎬 KirbyFeed</h1>

<p align="center">
  <b>一个能跑、能扛、能讲清楚的短视频 Feed 系统</b><br/>
  <span>Go + Gin · Vue 3 · MySQL · Redis · RabbitMQ · Docker</span>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.24.5-00ADD8?style=flat&logo=go" alt="Go"/>
  <img src="https://img.shields.io/badge/Vue-3-42B883?style=flat&logo=vue.js" alt="Vue"/>
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat" alt="License"/>
</p>

---

## 一句话介绍

> 国内 Go 后端圈里，能同时把 **Feed 流、分片续传、实时通知、热榜、事件驱动异步化** 都做扎实的开源项目不多——这个项目就是其中之一。
>
> 它不是「Hello World 级别的 demo」，而是一套**从接口到 Worker、从缓存到降级、从部署到可观测**都按生产级思路打磨过的短视频信息流系统。拿来学习能吃透架构，拿来面试能讲出取舍，拿来二开能直接长出新业务。

> 💡 想要更「全家桶」的版本？[LeoninCS/GCFeed](https://github.com/LeoninCS/GCFeed) 是更全面的视频 Feed 系统项目，业务能力覆盖更完整，建议一并围观。

---

## 它到底解决了什么「真问题」

写 Feed 系统最怕三件事：**缓存被打穿、热榜被老视频霸榜、分页翻着翻着就重复漏项**。这个项目没有用「加机器」糊弄过去，而是老老实实把每个坑都填了：

- **三级缓存 + 自愈**：本地缓存 → Redis → MySQL，热点 Key 失效也不打穿 DB；Redis 挂了自动降级直查，恢复后异步回填，接口零报错。
- **冷热分离时间线**：Redis 只留最近千条热时间线，深分页查询直接降为 0，翻页无重复无漏项。
- **Outbox 可靠投递**：把「写库」和「发消息」用单库事务锁死，MQ 故障期间安全积压、恢复后自动补发，绝不出现「视频发了但时间线没动」。
- **滑动窗口热榜**：热度按分钟分桶、合并近 60 桶，老视频不再永久霸榜；分钟级快照冻结排序键，翻页零抖动。
- **事件驱动异步化**：点赞/评论/关注/热度拆成独立消息由 Worker 消费，接口变轻，MQ 挂了也不丢写（三级重试 + 死信）。
- **分片上传与断点续传**：5MB 切片 + 逐片 MD5 校验 + 空洞补传，大文件失败不用从头再来。
- **双 Token 鉴权**：15 分钟 JWT + 7 天 Refresh Token，服务端可即时吊销；匿名能刷 Feed，登录能看个性化状态。

> 这些都是能在 `backend/internal/...` 里逐行对上的真实实现，不是 PPT 架构。

---

## 技术栈

| 维度 | 组件 | 说明 |
|------|------|------|
| 语言 | **Go 1.24** | 后端核心（API + Worker 双进程） |
| Web 框架 | **Gin** | 路由、参数绑定、统一返回、中间件链 |
| ORM | **GORM** | 模型定义 + `AutoMigrate` 自动建表 |
| 存储 | **MySQL** | 账号/视频/点赞/评论/关注/标签/私信/通知/Outbox |
| 缓存 | **Redis** | Token、Feed 时间线、视频实体、热榜、分片会话、限流 |
| 消息队列 | **RabbitMQ** | Topic 事件总线 + DLX 死信，支持异常降级直写 |
| 异步 | **Worker** | 点赞/评论/关注/热度四类消费者独立扩容 |
| 文件 | **本地磁盘 / 阿里云 OSS** | 直传 OSS + 预签名 URL，未配置则回落本地 |
| 容器化 | **Docker Compose** | 一键拉起全套依赖 + 应用 + 健康检查 |
| 前端 | **Vue 3 + Vite + Pinia** | 沉浸式播放、发布、主页、私信 |
| 可观测 | **pprof / healthz** | 健康检查 + 性能剖析 |

---

## 功能清单

| 模块 | 能力 |
|------|------|
| 账号 | 注册、登录、Refresh Token、改名、改密、登出、头像上传、个人简介、主页统计 |
| 视频 | 普通上传、5MB 分片上传、断点续传、封面上传、发布、作品列表、详情缓存、`#话题` 标签 |
| 点赞 | 点赞/取消、是否已赞、已赞列表、RabbitMQ 异步落库、热度更新、SSE 通知 |
| 评论 | 发布、删除、列表、`@username` 提及通知、异步落库、热度更新 |
| 关注 | 关注、取关、粉丝/关注列表、计数、SSE 通知 |
| Feed | 推荐流、关注流、热榜、话题流、冷热分离、游标分页、沉浸播放 |
| 私信 | 发送私信、按对端查看最近 50 条会话 |
| 通知 | 点赞/评论/关注/提及事件、SSE 实时推送、列表、未读计数、已读标记 |
| 工程 | Docker Compose、`start.sh`、API/Worker 拆分、限流、pprof、健康检查 |

---

## 🚀 一键启动

```bash
docker compose up -d --build
```

打开浏览器：

- 前端：`http://localhost:5173`
- 后端 API：`http://localhost:8080`
- RabbitMQ 管理台：`http://localhost:15672`（`admin` / `password123`）

> ⚠️ 生产环境务必显式设置 `JWT_SECRET`！缺失时后端会**拒绝启动**（不会偷偷生成随机密钥，否则重启后所有 token 失效、多实例互不认账）。

不想用 Docker？`start.sh` 也能跑：

```bash
./start.sh
# 常用开关
START_FRONTEND=0 ./start.sh   # 只起 API + Worker
START_WORKER=0   ./start.sh   # 只起 API + 前端
STOP_DOCKER=1    ./start.sh   # 退出时停止脚本拉起的 compose 服务
```

本地开发（分步）见下方：

```bash
# 1. 起依赖
docker compose up -d mysql redis rabbitmq

# 2. 后端
cd backend
CONFIG_PATH=configs/config.compose-local.yaml go run ./cmd
# Worker（另开一个终端）
CONFIG_PATH=configs/config.compose-local.yaml go run ./cmd/worker

# 3. 前端
cd frontend
npm install && npm run dev
```

---

## 🧠 架构亮点（简历/面试可直接用）

| 维度 | 亮点 | 一句话价值 |
|------|------|-----------|
| 缓存 | 三级缓存 + 自愈 | Redis 宕机不影响业务，恢复后自动热启动 |
| 缓存 | 分布式锁防击穿 | 热点 Key 过期不让并发打穿 MySQL |
| 缓存 | Feed 冷热分离 | 深分页降为 0，翻页不重不漏 |
| 缓存 | 滑动窗口热榜 | 老视频不霸榜，翻页零抖动 |
| 缓存 | 主动失效一致性 | 数据变了立刻清缓存，不看到旧数据 |
| 上传 | 分片上传 + 断点续传 | 大文件失败复用已传分片，省流量 |
| 实时 | SSE 通知推送 | 点赞/评论/关注实时到位，离线也能补看 |
| 分页 | 复合游标 + 快照分页 | 同毫秒多视频也不重不漏、不跳页 |
| 鉴权 | 双 Token + 软硬鉴权 | 可即时吊销，又能匿名刷 Feed |
| 稳定 | Redis 限流 + 多级降级 | 基础设施异常时核心业务仍可用 |
| 异步 | RabbitMQ 事件驱动 | 接口轻量、削峰填谷、易扩展 |
| 异步 | Outbox + 异常降级直写 | MQ 故障不丢写，最终一致 |
| 交付 | Docker Compose + 脚本 | 一套依赖、统一入口、可独立伸缩 |
| 质量 | AutoMigrate + pprof | 部署省心，运行时问题可定位 |

详细的模块设计、表结构、流程图、缓存与 MQ 设计文档见 [KirbyFeed项目设计.md](KirbyFeed项目设计.md)。

---

## 📡 API 速览

### 账号 `/account`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/register` | 否 | 注册（限流 5次/时/IP） |
| POST | `/login` | 否 | 登录，返回 access + refresh token |
| POST | `/refresh` | 否 | 刷新 access_token |
| POST | `/changePassword` | JWT | 改密（成功后吊销双 token） |
| POST | `/findByID` `/findByUsername` `/getProfile` | 否 | 查用户 / 查主页统计 |
| POST | `/logout` `/rename` | JWT | 登出 / 改名 |
| POST | `/uploadAvatar` `/updateProfile` | JWT | 头像 / 简介 |

### 视频 `/video`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/publish` | JWT | 发布（自动提取 #话题） |
| POST | `/uploadVideo` | JWT | 上传（mp4 ≤200MB） |
| POST | `/uploadCover` | JWT | 封面（≤10MB） |
| POST | `/chunk/init` `/chunk/upload` `/chunk/status` `/chunk/complete` | JWT | 分片上传 + 断点续传 |
| POST | `/listByAuthorID` `/getDetail` | 否 | 作品列表 / 详情缓存 |

### 互动 `/like` `/comment` `/social`
| 模块 | 方法 | 路径 | 说明 |
|------|------|------|------|
| like | POST | `/like` `/unlike` `/isLiked` `/listMyLikedVideos` | 点赞、取消、是否已赞、已赞列表 |
| comment | POST | `/listAll` `/publish` `/delete` | 评论列表、发布（支持 @提及）、删除 |
| social | POST | `/follow` `/unfollow` `/getAllFollowers` `/getAllVloggers` `/getCounts` | 关注、取关、粉丝/关注列表、计数 |

### Feed `/feed`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| POST | `/listLatest` | 软鉴权 | 最新流（游标分页） |
| POST | `/listByPopularity` | 软鉴权 | 热度榜（快照分页） |
| POST | `/listByFollowing` | JWT | 关注流 |
| POST | `/listTags` `/listByTag` | 软鉴权 | 热门话题 / 按 #话题 浏览 |

### 通知 `/notification` · 私信 `/message`
| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| GET | `/notification/stream?token=<token>` | 是 | SSE 实时推送 |
| POST | `/notification/list` `/markRead` `/unreadCount` | 是 | 列表 / 已读 / 未读计数 |
| POST | `/message/send` `/message/list` | JWT | 发私信 / 会话列表 |

完整接口契约见原 [README 设计文档](KirbyFeed项目设计.md)。

---

## 🔧 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `JWT_SECRET` | 无（缺失即启动失败） | JWT 签名密钥，建议 ≥32 字节 |
| `SERVER_PORT` | `8080` | 后端端口 |
| `MYSQL_*` | 配置文件值 | host/port/user/password/database/root_password |
| `REDIS_*` | 配置文件值 | host/port/password/db |
| `RABBITMQ_*` | `admin` / `password123` | host/port/user/pass |
| `OBJECT_STORAGE_ENABLED` | `0` | 启用对象存储（默认本地磁盘） |
| `OSS_*` | 无 | 阿里云 OSS 配置（endpoint/bucket/key 等） |

详见 `.env.example`。

---

## 🧪 CI

GitHub Actions（`.github/workflows/ci.yml`）在 PR 及推送到 `main`/`master` 时运行：

- 后端：Go 1.24.x，`go mod download` → `go vet ./...` → `go test -race -count=1 ./...`
- 前端：Node.js 22，`npm ci` → `npm run build`

---

## 📈 可观测性

- `GET /healthz` —— 健康检查
- pprof：API `localhost:6060`，Worker `localhost:6061`
- Redis 用途：Token 缓存、视频实体缓存、Feed 时间线、热榜窗口、分片会话
- RabbitMQ：Topic Exchange 覆盖点赞/评论/关注/热度/时间线事件，配 DLX

---

## 📂 目录结构

```
KirbyFeed/
├── backend/          # Go 后端（API + Worker）
├── frontend/         # Vue 3 前端
├── picture/          # 架构图 / 表结构图
├── test/             # Postman 集合
├── 面试/             # 面试展开版文档
├── docker-compose.yml
├── start.sh
└── KirbyFeed项目设计.md
```

---

## 🤝 适合谁看

- **想系统学 Go 后端架构的同学**：每一处设计都能在代码里找到落点。
- **准备后端面试的同学**：亮点清单直接能用，面试文档目录已备好。
- **想做短视频/Feed 类产品的团队**：拿来二开，长出自己的业务。

> 如果这个项目对你有帮助，点个 ⭐ 就是对作者最大的鼓励。也欢迎提 Issue 和 PR——好的 Go 后端项目，是社区一起喂出来的。

## 📄 License

MIT © KirbyFeed
