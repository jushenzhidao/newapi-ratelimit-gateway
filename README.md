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
| `RATELIMIT_ON_REDIS_ERROR` | Redis 故障降级策略：`passthrough` / `reject` / `local_fallback`（默认） |
| `RATELIMIT_FALLBACK_LIMIT_DIVISOR` | 分组感知兜底除数（0 = 自动用 `SERVER_WORKERS`） |
| `RATELIMIT_CIRCUIT_BREAKER_*` | 熔断器：开关 / 失败阈值（默认 5）/ 打开秒数（默认 10） |
| `RATELIMIT_LOCAL_CACHE_*` | L1 进程内缓存：容量（默认 5 万）/ TTL（默认 60s）/ 负缓存 TTL |
| `RATELIMIT_QUOTA_BATCH_SIZE` | 批量配额预取大小（默认 10，Redis QPS 降为 1/N） |

> `.env` 含真实凭据，已被 `.gitignore` 排除，请勿提交。

## 高可用架构

限速链路按四层可靠性设计：

```
请求 → [熔断器] → [L1 缓存 → Redis → MySQL 读透]  (keymap/config 解析)
     → [批量配额预取: 一次 EVALSHA 取 N 个配额本地消耗]  (请求数计数)
     → [进程内滑动窗口兜底]  (Redis 故障期间)
```

- **三级解析**：key→分组、分组→策略 的查找走 L1 进程内缓存（LRU+TTL）→ Redis → MySQL 读透（读透结果写回 Redis）。Redis 挂掉时分组判断依然准确；负缓存防止恶意 key 穿透到数据库。
- **熔断器**：Redis 连续失败 5 次后熔断打开，请求立即走降级路径，**不再逐请求等待 Redis 超时**（避免延迟雪崩与连接池耗尽）；10 秒后半开探测，恢复自动闭合。`/health` 的 ping 会反哺熔断状态。
- **批量配额预取**（请求数模式）：每 worker 一次 EVALSHA 预取 `RATELIMIT_QUOTA_BATCH_SIZE` 个配额本地消耗（flush + 检查 + 预取单次往返完成）。Redis QPS 降为 1/N，且短暂抖动期间（N 个请求内）限速完全不感知。多 worker 并发预取的超发上界约 `(workers-1) × batch_size`，长窗口（5h/7d/30d）场景可接受。
- **分组感知兜底**：`local_fallback` 模式下，已解析出分组策略的 key 按「5h 限额 ÷ worker 数」执行本地滑动窗口（默认除数 = `SERVER_WORKERS`，可用 `RATELIMIT_FALLBACK_LIMIT_DIVISOR` 覆盖）；解析不到策略时退回全局兜底参数。

### Redis 故障降级

Redis 是限速的唯一后端，故障时的行为由 `RATELIMIT_ON_REDIS_ERROR` 控制：

| 策略 | 行为 | 适用场景 |
|---|---|---|
| `passthrough` | 全部放行，限速失效 | 可用性优先，上游额度便宜 |
| `reject` | 返回 503 + `Retry-After: 10` | 保护上游优先，怕超额结算 |
| `local_fallback`（默认） | 进程内滑动窗口兜底限速 | 兼顾可用性与止损（推荐） |

其他保障：

- **NOSCRIPT 自愈**：Redis 重启后 Lua 脚本缓存丢失，网关自动重新 `SCRIPT LOAD` 并重试，限速自动恢复，无需重启网关。
- **`/health` 可观测**：返回 `status`（ok/degraded）、`redis`（up/down）、降级累计计数、熔断器状态（`circuit.state`）与三级解析统计（`resolver_*`）。Redis down 时仍返回 200，避免被负载均衡摘除；告警请按 `status == "degraded"` 或 `circuit.state != "closed"` 触发。
- **恢复自愈**：keymap / 分组配置由 MySQL 定时同步自动回填（读透路径也会即时写回）；计数器丢失会重置（Redis 开 AOF `appendonly yes` 可把丢失窗口压到 1 秒）。
- 全局兜底参数（`RATELIMIT_FALLBACK_WINDOW` / `RATELIMIT_FALLBACK_MAX_REQUESTS`）仅在解析不到分组策略时使用；每个 worker 独立计数，请保守设置。

## 冒烟测试

```bash
uv run python scripts/smoke_ha.py   # 熔断/L1/读透/批量配额/兜底 六组断言
```

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
