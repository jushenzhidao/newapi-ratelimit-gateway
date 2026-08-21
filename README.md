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

## 生产上线：无缝切换方案

### 场景

客户端已直接调用 `https://api-mall.chatfire.cn/v1/chat/completions`（NewAPI 公网域名，**前端 UI 也在此域名上**）。目标：无感切换到限速网关，API 调用走网关限速，前端 UI 不受影响，客户端零改动。

### 架构

```
Client ──> Nginx (api-mall.chatfire.cn)
             ├─ /v1/*  ──> 限速网关 (host:39778) ──> NEWAPI_BASE_URL (NewAPI 内部地址)
             └─ /*     ──> NewAPI 前端 (原上游，不经网关)
```

⚠️ **核心原则：`NEWAPI_BASE_URL` 必须指向 NewAPI 的内部地址**（如 `http://127.0.0.1:3000`），**绝不能**写成公网域名 `https://api-mall.chatfire.cn/v1`——否则 Nginx 将 `/v1/` 路由到网关后，网关又请求公网域名，请求再次回到 Nginx → 网关 → 死循环。

### Nginx 配置

在 `api-mall.chatfire.cn` 的 server block 中新增 `/v1/` 分流（其余保持原样）：

```nginx
server {
    listen 443 ssl;
    server_name api-mall.chatfire.cn;

    # ---- API 调用 → 限速网关 ----
    location /v1/ {
        proxy_pass http://127.0.0.1:39778;   # 限速网关地址（按实际内网 IP 调整）

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # 流式响应必须关闭缓冲
        proxy_buffering off;
        proxy_cache off;
        proxy_http_version 1.1;
        proxy_set_header Connection "";

        # LLM 流式调用耗时较长
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # ---- 其余请求 → NewAPI 前端（原配置不变）----
    location / {
        proxy_pass http://127.0.0.1:3000;   # NewAPI 原上游
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

> 若网关与 NewAPI 不在同一台机器：`proxy_pass` 指向网关内网 IP，`NEWAPI_BASE_URL` 指向 NewAPI 内网 IP（如 `http://10.0.1.5:3000`）。

### 网关 .env 关键配置

```bash
NEWAPI_BASE_URL=http://127.0.0.1:3000   # NewAPI 内部地址，禁止写公网域名
SERVER_HOST=0.0.0.0
SERVER_PORT=39778                       # 与 Nginx proxy_pass 一致
```

### 切换步骤

1. 启动网关，确认 `NEWAPI_BASE_URL` 指向 NewAPI 内部地址
2. 直接压测网关：`curl http://127.0.0.1:39778/v1/chat/completions -H "Authorization: Bearer sk-xxx"`，确认限速生效、请求能透传到 NewAPI
3. 修改 Nginx 配置，`nginx -t && nginx -s reload`
4. 验证：客户端调用 `https://api-mall.chatfire.cn/v1/chat/completions`（零改动），检查 `X-RateLimit-*` 响应头或 `/ratelimit/status/{key}` 确认已走网关
5. 前端访问 `https://api-mall.chatfire.cn/` 确认不受影响

### 回滚方案

改回 Nginx（把 `location /v1/` 的 `proxy_pass` 指回 NewAPI 原上游）并 reload，客户端即刻恢复直连 NewAPI，无需动客户端配置。

## Web 管理后台

项目自带一个现代化管理后台（`./web/`，纯静态 SPA，无需构建），覆盖管理 API 全功能：分组 CRUD、启停、手动同步、Key 限速状态查询。

### 访问方式

网关已内置静态挂载，启动后直接访问：

```
http://<网关地址>:<SERVER_PORT>/web/
```

Docker 部署时 `COPY . .` 已包含 `web/` 目录，无需额外配置。也可以单独用任意静态服务器托管 `./web/`（登录时填网关地址即可跨域访问，网关默认开放 CORS）。

### 登录

- 填写**网关地址**（如 `http://127.0.0.1:8080`）和 **管理 Token**（`ADMIN_AUTH_TOKEN`）
- 登录即调用 `GET /admin/groups` 验证：**请求成功即鉴权通过**，Token 仅保存在浏览器 localStorage
- 会话自动探测：刷新页面时若 Token 仍有效直接进入主界面

### 功能

| 功能 | 对应接口 |
|---|---|
| 新建 / 编辑 / 禁用 / 启用分组 | `POST` / `PUT` / `DELETE /admin/groups[...]` |
| 手动同步 Key→分组映射 | `POST /admin/sync` |
| Key 限速状态查询（含配额使用率进度条） | `GET /ratelimit/status/{api_key}` |
| 明暗主题切换、Toast 提示、表单校验 | — |

### 安全说明

- 网关 CORS 默认允许任意来源（`ADMIN_CORS_ORIGINS=*`）；鉴权完全依赖 Bearer Token（无 Cookie，不受 CSRF 影响），风险可控
- 生产环境建议用 `ADMIN_CORS_ORIGINS` 收紧为管理台实际域名，并在 Nginx 层为 `/web/` 与 `/admin/*` 增加访问控制（如 IP 白名单 / 基础认证）

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
| `ADMIN_CORS_ORIGINS` | 管理后台跨域白名单，逗号分隔；默认 `*`（生产建议收紧） |
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
web/            # 管理后台静态页面（网关 /web 挂载）
.env.example    # 环境变量配置模板
```
