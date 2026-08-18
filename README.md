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
cp config.example.yaml config.yaml   # 按实际环境修改配置
uv run uvicorn app.main:app --port 8080
```

依赖：Redis、MySQL（执行 `sql/init.sql` 初始化）。

### Docker 部署

```bash
docker pull <dockerhub_username>/newapi-ratelimit-gateway:latest

# 或从源码构建
docker compose up -d
```

`docker-compose.yml` 会同时启动网关与 Redis；`config.yaml` 与 `lua/` 通过挂载卷提供。

## 配置

复制 `config.example.yaml` 为 `config.yaml` 并填写：

- `newapi.base_url`：NewAPI 后端地址
- `redis` / `mysql` / `newapi_mysql`：连接信息与密码
- `admin.auth_token`：管理 API 认证 token（生产环境必须使用强随机值）

> `config.yaml` 含真实凭据，已被 `.gitignore` 排除，请勿提交。

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
config.example.yaml  # 配置模板
```
