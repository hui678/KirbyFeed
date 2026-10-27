# 终端启动项目

当前环境为 Windows，以下命令默认在 **PowerShell** 中执行，工作目录为项目根目录：

```powershell
cd d:\fronted-projects\feedsystem_video_go-main
```

---

## 0. 前置条件

| 组件 | 版本要求 | 检查命令 |
|------|----------|----------|
| Go | 1.24.5+ | `go version` |
| Node.js | 20+ | `node -v` |
| npm | 10+ | `npm -v` |
| Docker Desktop | 已安装并启动 | `docker ps` |

如果 `go` 提示“不是内部或外部命令”，说明 Go 未安装或未加入 PATH，安装后需重新打开终端：
<https://go.dev/dl/>

如果 `docker ps` 报 `failed to connect to the docker API ... dockerDesktopLinuxEngine`，说明 Docker Desktop 没启动，先双击打开 Docker Desktop，等状态变为 Running。

---

## 1. 启动依赖（MySQL / Redis / RabbitMQ）

```powershell
docker compose up -d mysql redis rabbitmq
```

查看是否启动成功：

```powershell
docker compose ps
```

依赖端口与账号（来自 [docker-compose.yml](docker-compose.yml)）：

| 服务 | 宿主机端口 | 账号 |
|------|-----------|------|
| MySQL | `3307`（容器内 3306） | `root` / `123456`，库名 `feedsystem` |
| Redis | `6379` | 密码 `123456` |
| RabbitMQ | `5672`，管理台 `15672` | `admin` / `password123` |

> 注意：MySQL 映射到宿主机 **3307**，不是 3306，避免和本机已有 MySQL 冲突。后端配置 [config.compose-local.yaml](backend/configs/config.compose-local.yaml) 已按 3307 配置。

RabbitMQ 管理台：<http://localhost:15672>

---

## 2. 启动后端 API

**必须进入 backend 目录**，因为配置、`.env`、上传目录都是相对路径：

```powershell
cd d:\fronted-projects\feedsystem_video_go-main\backend
$env:CONFIG_PATH = "configs/config.compose-local.yaml"
go run ./cmd
```

看到以下日志即启动成功：

```text
Config loaded from file: configs/config.compose-local.yaml
Redis connected (cache enabled)
RabbitMQ connected
Server is running on port 8080
```

验证：

```powershell
Invoke-RestMethod http://127.0.0.1:8080/healthz
```

---

## 3. 启动 Worker（异步消费进程）

**另开一个终端**，同样进入 backend 目录：

```powershell
cd d:\fronted-projects\feedsystem_video_go-main\backend
$env:CONFIG_PATH = "configs/config.compose-local.yaml"
go run ./cmd/worker
```

看到各 Worker 的 `started, consuming` 日志即正常。

> Worker 强依赖 MySQL 与 RabbitMQ，连接失败会重试 10 次后直接退出。
> API 进程是可降级的：Redis / RabbitMQ 连不上仍可启动，但对应功能关闭。

---

## 4. 启动前端

**再开一个终端**：

```powershell
cd d:\fronted-projects\feedsystem_video_go-main\frontend
npm install
npm run dev
```

打开 <http://localhost:5173>。

前端通过 [vite.config.ts](frontend/vite.config.ts) 把 `/api` 代理到 `http://127.0.0.1:8080`，所以后端必须先启动。

---

## 5. 一键启动全部服务（Docker Compose）

不需要本地安装 Go / Node：

```powershell
cd d:\fronted-projects\feedsystem_video_go-main
Copy-Item .env.example .env -ErrorAction SilentlyContinue
docker compose up -d --build
```

访问地址：

- 前端：<http://localhost:5173>
- 后端 API：<http://localhost:8080>
- RabbitMQ 管理台：<http://localhost:15672>

查看日志：

```powershell
docker compose logs -f backend
docker compose logs -f worker
```

---

## 6. Git Bash 一键脚本（可选）

如果你使用 Git Bash，可以直接跑项目自带的 [start.sh](start.sh)：

```bash
cd /d/fronted-projects/feedsystem_video_go-main
./start.sh
```

常用开关：

```bash
START_FRONTEND=0 ./start.sh   # 只启动 API + Worker
START_WORKER=0 ./start.sh     # 只启动 API + 前端
STOP_DOCKER=1 ./start.sh      # 退出时停止 compose 服务
```

---

## 7. 常用环境变量（可选）

本地直跑时，如果不想用写死的配置，可以在 `backend` 目录下放 `.env`（可以复制根目录的 `.env.example`）。

PowerShell 临时覆盖示例：

```powershell
$env:MYSQL_PORT = "3307"
$env:MYSQL_ROOT_PASSWORD = "123456"
$env:REDIS_PASSWORD = "123456"
$env:RABBITMQ_USER = "admin"
$env:RABBITMQ_PASS = "password123"
$env:JWT_SECRET = "feedsystem-dev-secret-key"
$env:SERVER_PORT = "8080"
```

> `.env` 必须放在 **backend 目录**，因为 `godotenv.Load()` 读取的是当前工作目录。

---

## 8. 停止与清理

只停止本地 `go run` / `npm run dev`：在各终端按 `Ctrl + C`。

停止依赖容器：

```powershell
docker compose stop mysql redis rabbitmq
```

停止并删除容器（保留数据卷）：

```powershell
docker compose down
```

连同数据库数据一起删除：

```powershell
docker compose down -v
```

---

## 9. 常见问题

| 现象 | 原因与处理 |
|------|-----------|
| `go: command not found` / `不是内部或外部命令` | Go 未安装或未加入 PATH，安装后重开终端 |
| `Failed to connect database` | MySQL 容器未就绪或端口不对；确认 `docker compose ps` 中 mysql 为 healthy，端口用 3307 |
| 日志出现 `Redis not available (cache disabled)` | Redis 未启动或密码不对；确认密码为 `123456` |
| `RabbitMQ config error (disabled)` | RabbitMQ 未启动；`docker compose up -d rabbitmq` |
| 本机已有 MySQL 占用 3306 | 项目 compose 用 3307 映射，不要改成 3306；若用本机 MySQL，需设 `MYSQL_PORT=3306` |
| 前端接口 502 / ECONNREFUSED | 后端未启动，或后端端口不是 8080 |
| `localhost` 解析到 IPv6 导致连接失败 | 统一使用 `127.0.0.1`，前端代理已强制 IPv4 |
| 视频上传后访问不到 | 上传目录为 `backend/.run/uploads`，需从 backend 目录启动 |
