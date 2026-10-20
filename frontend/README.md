# feedsystem_video_go frontend

Vue 3 + Vite 前端，面向 `backend/` 的短视频 Feed 应用。默认通过 Vite 代理把 `/api/...` 转发到 `http://localhost:8080/...`。

## 页面

| 路由 | 页面 | 说明 |
|------|------|------|
| `/` | 推荐页 | 沉浸式短视频播放，支持推荐、关注、点赞榜三个 Tab，本地搜索标题/作者，滚动加载。 |
| `/hot` | 热榜 | 对接 `/feed/listByPopularity`，支持刷新与加载更多。 |
| `/video` | 发布页 | 登录后可发布视频；视频按 5MB 分片上传，支持 MD5 校验、并发上传、失败重试、断点续传。 |
| `/video/:id` | 视频详情 | 视频播放、点赞、评论抽屉、作者信息。 |
| `/account` | 账号页 | 登录、个人信息、粉丝/关注入口。 |
| `/account/register` | 注册页 | 创建账号。 |
| `/account/change-password` | 改密页 | 使用旧密码修改密码。 |
| `/settings` | 设置页 | 改名、退出登录、跳转改密。 |
| `/u/:id` | 用户主页 | 用户作品、粉丝/关注列表、关注/取关、私信入口。 |
| `/messages` | 私信联系人 | 从粉丝和关注用户中选择聊天对象。 |
| `/messages/:peerId` | 私信会话 | 查看最近 50 条私信并发送消息。 |

## 前端能力

- Pinia 保存 access token 与 refresh token。
- API Client 在 401 时自动调用 `/account/refresh` 并重试原请求。
- 发布页使用 `spark-md5` 计算文件 MD5 与分片 MD5。
- 播放页使用虚拟渲染窗口，只保留当前视频前后各一条 DOM，降低长列表播放成本。
- 点赞、关注、评论、分享等交互通过 toast 提示结果。
- Vite 代理地址可通过 `VITE_API_BASE` 覆盖。

## 开发启动

先启动后端 API：

```bash
cd backend
CONFIG_PATH=configs/config.compose-local.yaml go run ./cmd
```

启动前端：

```bash
cd frontend
npm install
npm run dev
```

完整链路可在项目根目录执行：

```bash
./start.sh
```

## 构建

```bash
npm run build
```

Docker Compose 中前端容器使用 Nginx 托管构建产物，并把 `/api` 反向代理到后端服务。
