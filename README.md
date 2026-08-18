# newapi-ratelimit-gateway

NewAPI 无侵入式限速网关：客户端 → 本网关 → NewAPI → 上游 LLM。基于 Redis 滑动窗口 + Lua 原子脚本实现多窗口限速，支持按请求数 / 按 Token 用量两种模式，支持分组策略。

## 特性

- 5h / 7d / 30d 三窗口限速（Redis ZSET 滑动窗口 + Lua 原子脚本）
- 两种限速类型：`request`（请求数）/ `token`（Token 用量）
- 两种限速粒度：按 Key 独立 / 按分组共享
- 分组策略管理 API（MySQL 持久化，Redis 缓存）
- 流式 SSE / tool call 完全透传兼容
- 无侵入：只需把客户端 base_url 指向网关

## 快速开始

### 本地运行（uv 管理环境）

```bash
uv sync
cp .env.example .env   # 按实际环境修改配置
uv run uvicorn app.main:app --port 8080
```

依赖：**外部** Redis 与 MySQL（本项目不自带部署，需自行准备；MySQL 执行 `sql/init.sql` 初始化）。

### Docker 部署

```bash
docker pull <dockerhub_username>/newapi-ratelimit-gateway:latest

# 或从源码构建（配置全部来自 .env）
cp .env.example .env && docker compose up -d
```

容器内 host/port/workers 由 `SERVER_HOST` / `SERVER_PORT` / `SERVER_WORKERS` 控制；修改 `SERVER_PORT` 时记得同步 `docker-compose.yml` 的端口映射。

## 配置（.env 环境变量）

复制 `.env.example` 为 `.env` 并填写，优先级：**环境变量 > .env 文件 > 代码默认值**。主要变量：

| 变量 | 说明 |
|---|---|
| `NEWAPI_BASE_URL` | NewAPI 后端地址 |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` | 外部 Redis 连接 |
| `MYSQL_HOST` / `MYSQL_USER` / `MYSQL_PASSWORD` | 外部 MySQL（网关配置库） |
| `NEWAPI_MYSQL_*` | NewAPI 数据库（只读，同步 Key→Group） |
| `SERVER_WORKERS` | uvicorn worker 数 |
| `ADMIN_AUTH_TOKEN` | 管理 API 认证 token（生产必须强随机值） |

> `.env` 含真实凭据，已被 `.gitignore` 排除，请勿提交。

## CI / 镜像发布

推送到 `main` 分支或打 `v*` 标签时，GitHub Actions 自动构建 Docker 镜像并推送至 DockerHub（PR 仅构建不推送）。

在仓库 Settings → Secrets and variables → Actions 中配置：

| Secret | 说明 |
|---|---|
| `DOCKERHUB_USERNAME` | DockerHub 用户名 |
| `DOCKERHUB_TOKEN` | DockerHub Access Token（https://hub.docker.com → Account Settings → Security 生成） |

镜像标签规则：`main` 分支 → `latest` + `main-<sha>`；`v1.2.3` 标签 → `1.2.3` / `1.2` / `1` + `latest`。

## 项目结构

```
app/            # FastAPI 应用（代理、限速、同步、管理 API）
lua/            # Redis Lua 原子脚本
sql/            # 数据库初始化脚本
.env.example    # 环境变量配置模板
```
