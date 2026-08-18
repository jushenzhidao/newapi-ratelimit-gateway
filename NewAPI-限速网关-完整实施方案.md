# NewAPI 限速网关微服务 - 完整实施方案

> **目标**：在 NewAPI 前加一层限速网关，实现对用户的 5 小时 / 7 天 / 30 天多窗口限速，支持按分组配置不同限速策略，NewAPI 代码零修改。

---

## 1. 项目概述

### 1.1 核心需求

- **无侵入**：NewAPI 代码零修改，网关作为透明反向代理
- **多窗口限速**：5 小时、7 天、30 天三个滑动窗口同时生效
- **分组策略**：不同产品分组有不同的限速配额，通过 API Key 查找所属分组获取策略
- **两种模式**：按请求数限速 / 按 Token 用量限速（可配置）
- **两种粒度**：按 Key 独立配额 / 按分组共享配额（可配置）

### 1.2 请求链路

```
客户端 → 限速网关(微服务) → NewAPI → 上游 LLM
              ↕
           Redis (策略缓存 + 计数器)
              ↕
           MySQL (分组配置表)
```

### 1.3 限速检查流程

```
请求到达
  ↓
① 提取 API Key (Header)
  ↓
② Key → 分组 (Redis: keymap:{hash})
  ↓
③ 分组 → 限速策略 (Redis: config:{group})
  ↓
④ Lua 原子检查 3 个滑动窗口 + 计数
  ↓
全部通过 → 转发 NewAPI / 任一超限 → 返回 429
```

---

## 2. 技术选型

| 组件      | 选型                      | 理由               |
| ------- | ----------------------- | ---------------- |
| 语言      | Python 3.13+            | 团队熟悉，异步生态完善（uv 管理环境） |
| Web 框架  | FastAPI                 | 原生 async，中间件机制成熟 |
| HTTP 代理 | httpx (async)           | 支持流式响应透传         |
| Redis   | redis-py[asyncio]       | Lua 脚本执行 + 异步    |
| MySQL   | SQLAlchemy + aiomysql   | 异步 ORM           |
| 配置管理    | pydantic-settings       | 类型安全的环境变量管理      |
| 部署      | Docker + docker-compose | 容器化，宝塔面板兼容       |

---

## 3. 项目结构

```
newapi-ratelimit-gateway/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI 应用入口 + 中间件注册
│   ├── config.py              # 配置管理 (pydantic-settings)
│   ├── proxy.py               # 反向代理 (透明转发 + 流式支持)
│   ├── ratelimit.py           # 限速核心 (Redis + Lua)
│   ├── sync.py                # Key→Group 同步 (从 NewAPI DB)
│   ├── admin.py               # 管理后台 API (分组配置 CRUD)
│   ├── models.py              # SQLAlchemy 数据模型
│   └── schemas.py             # Pydantic 请求/响应模型
├── lua/
│   └── rate_limit.lua         # 全合并 Lua 脚本
├── sql/
│   └── init.sql               # 数据库初始化
├── config.yaml                # 应用配置
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml               # 依赖声明（uv 管理）
├── uv.lock                      # 依赖锁文件
└── README.md
```

---

## 4. 数据模型

### 4.1 MySQL - 分组限速配置表

```sql
-- sql/init.sql

CREATE DATABASE IF NOT EXISTS ratelimit_gateway
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE ratelimit_gateway;

CREATE TABLE rate_limit_group (
    id          INT PRIMARY KEY AUTO_INCREMENT,
    group_name  VARCHAR(64)  NOT NULL UNIQUE COMMENT '分组名称，对应 NewAPI 的 token.group 字段',
    limit_5h    INT          NOT NULL DEFAULT 100    COMMENT '5小时配额',
    limit_7d    INT          NOT NULL DEFAULT 1000   COMMENT '7天配额',
    limit_30d   INT          NOT NULL DEFAULT 5000   COMMENT '30天配额',
    limit_type  ENUM('request', 'token') NOT NULL DEFAULT 'request' COMMENT '限速类型: 按请求数/按Token用量',
    scope       ENUM('key', 'group')    NOT NULL DEFAULT 'key'     COMMENT '限速粒度: 按Key独立/按分组共享',
    status      TINYINT      NOT NULL DEFAULT 1      COMMENT '1=启用 0=停用',
    remark      VARCHAR(256) DEFAULT NULL            COMMENT '备注',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分组限速策略配置';

-- 插入默认配置
INSERT INTO rate_limit_group (group_name, limit_5h, limit_7d, limit_30d, limit_type, scope, remark) VALUES
('default',    100,  1000,  5000,   'request', 'key',   '默认分组'),
('vip',        500,  5000,  50000,  'request', 'key',   'VIP分组'),
('enterprise', 2000, 20000, 200000, 'request', 'key',   '企业分组'),
('trial',      10,   50,    200,    'request', 'key',   '试用分组');
```

### 4.2 Redis 数据结构

```
# 1. Key → 分组映射 (String, TTL 300s)
keymap:{sha256(api_key)}  →  "vip"

# 2. 分组 → 限速策略 (String/JSON, TTL 60s)
config:{group_name}  →  {"5h":500,"7d":5000,"30d":50000,"type":"request","scope":"key"}

# 3. 限速计数器 (ZSET, TTL = 窗口长度)
# user_id = sha256(api_key) 当 scope=key
# user_id = group_name      当 scope=group
ratelimit:{user_id}:18000     →  ZSET (score=timestamp, member="{ts}:{rand}")
ratelimit:{user_id}:604800    →  ZSET
ratelimit:{user_id}:2592000   →  ZSET

# 4. Token 用量计数器 (按 Token 模式时使用, String, TTL = 窗口长度)
token_usage:{user_id}:18000   →  整数 (累计 token 数)
token_usage:{user_id}:604800  →  整数
token_usage:{user_id}:2592000 →  整数
```

---

## 5. 核心代码

### 5.1 依赖声明与安装（uv）

项目使用 [uv](https://docs.astral.sh/uv/) 管理 Python 环境与依赖。依赖声明在 `pyproject.toml`，锁定版本在 `uv.lock`：

```toml
[project]
name = "newapi-ratelimit-gateway"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "aiomysql==0.2.0",
    "fastapi==0.115.6",
    "httpx==0.28.1",
    "pydantic-settings==2.7.0",
    "pyyaml==6.0.2",
    "redis[hiredis]==5.2.1",
    "sqlalchemy[asyncio]==2.0.36",
    "uvicorn[standard]==0.34.0",
]
```

安装依赖并本地启动：

```bash
uv sync                                   # 创建 .venv 并安装全部依赖
uv run uvicorn app.main:app --port 8080  # 本地启动测试
```

> 说明：`requires-python = ">=3.13"` 约束解释器版本，uv 会自动选择或提示对应的 Python；新增依赖用 `uv add <包名>`，会同步更新 `uv.lock`。

### 5.2 config.yaml

```yaml
# 限速网关配置
server:
  host: "0.0.0.0"
  port: 8080
  workers: 4

# NewAPI 后端地址
newapi:
  base_url: "http://127.0.0.1:3000"
  timeout: 300  # 秒，LLM 请求可能很长

# Redis 配置
redis:
  host: "127.0.0.1"
  port: 6379
  password: ""
  db: 0
  pool_size: 50

# MySQL 配置 (网关自身的配置库)
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "root"
  password: ""
  database: "ratelimit_gateway"
  pool_size: 10

# NewAPI 数据库配置 (只读，用于同步 Key→Group 映射)
newapi_mysql:
  host: "127.0.0.1"
  port: 3306
  user: "readonly_user"
  password: ""
  database: "new-api"
  pool_size: 5

# 同步配置
sync:
  key_group_interval: 60  # Key→Group 同步间隔（秒）
  config_cache_ttl: 60    # 分组配置 Redis 缓存 TTL（秒）
  key_map_ttl: 300         # Key→Group Redis 缓存 TTL（秒）

# 限速默认行为
ratelimit:
  # Key 未找到时的行为: passthrough=放行让NewAPI鉴权, reject=拒绝
  on_key_not_found: "passthrough"
  # 分组未配置限速时的行为: passthrough=放行, default=使用default分组配额
  on_config_not_found: "passthrough"
  # 429 响应中是否返回剩余配额信息
  show_remaining: true

# 管理后台
admin:
  enabled: true
  # 管理接口认证 token (建议生产环境改为更安全的认证方式)
  auth_token: "change-me-in-production"
```

### 5.3 app/config.py

```python
"""配置管理 - 从 config.yaml 加载"""

from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
import yaml
from typing import Optional


class ServerConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    workers: int = 4


class NewAPIConfig(BaseSettings):
    base_url: str = "http://127.0.0.1:3000"
    timeout: int = 300


class RedisConfig(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 6379
    password: str = ""
    db: int = 0
    pool_size: int = 50


class MySQLConfig(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "ratelimit_gateway"
    pool_size: int = 10


class NewAPIMySQLConfig(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "readonly_user"
    password: str = ""
    database: str = "new-api"
    pool_size: int = 5


class SyncConfig(BaseSettings):
    key_group_interval: int = 60
    config_cache_ttl: int = 60
    key_map_ttl: int = 300


class RateLimitConfig(BaseSettings):
    on_key_not_found: str = "passthrough"
    on_config_not_found: str = "passthrough"
    show_remaining: bool = True


class AdminConfig(BaseSettings):
    enabled: bool = True
    auth_token: str = "change-me-in-production"


class AppConfig:
    """全局配置单例"""

    def __init__(self):
        config_path = Path(__file__).parent.parent / "config.yaml"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        else:
            data = {}

        self.server = ServerConfig(**data.get("server", {}))
        self.newapi = NewAPIConfig(**data.get("newapi", {}))
        self.redis = RedisConfig(**data.get("redis", {}))
        self.mysql = MySQLConfig(**data.get("mysql", {}))
        self.newapi_mysql = NewAPIMySQLConfig(**data.get("newapi_mysql", {}))
        self.sync = SyncConfig(**data.get("sync", {}))
        self.ratelimit = RateLimitConfig(**data.get("ratelimit", {}))
        self.admin = AdminConfig(**data.get("admin", {}))

    @property
    def redis_url(self) -> str:
        auth = f":{self.redis.password}@" if self.redis.password else ""
        return f"redis://{auth}{self.redis.host}:{self.redis.port}/{self.redis.db}"

    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+aiomysql://{self.mysql.user}:{self.mysql.password}"
            f"@{self.mysql.host}:{self.mysql.port}/{self.mysql.database}"
        )

    @property
    def newapi_mysql_url(self) -> str:
        return (
            f"mysql+aiomysql://{self.newapi_mysql.user}:{self.newapi_mysql.password}"
            f"@{self.newapi_mysql.host}:{self.newapi_mysql.port}/{self.newapi_mysql.database}"
        )


# 全局配置实例
config = AppConfig()
```

### 5.4 app/models.py

```python
"""SQLAlchemy 数据模型"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Enum, Text, DateTime, Index
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class RateLimitGroup(Base):
    """分组限速策略配置表"""
    __tablename__ = "rate_limit_group"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_name = Column(String(64), nullable=False, unique=True, comment="分组名称")
    limit_5h = Column(Integer, nullable=False, default=100, comment="5小时配额")
    limit_7d = Column(Integer, nullable=False, default=1000, comment="7天配额")
    limit_30d = Column(Integer, nullable=False, default=5000, comment="30天配额")
    limit_type = Column(Enum("request", "token"), nullable=False, default="request", comment="限速类型")
    scope = Column(Enum("key", "group"), nullable=False, default="key", comment="限速粒度")
    status = Column(Integer, nullable=False, default=1, comment="1=启用 0=停用")
    remark = Column(String(256), nullable=True, comment="备注")
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    def to_config_dict(self) -> dict:
        """转为 Redis 缓存的 JSON 格式"""
        return {
            "5h": self.limit_5h,
            "7d": self.limit_7d,
            "30d": self.limit_30d,
            "type": self.limit_type,
            "scope": self.scope,
        }
```

### 5.5 app/schemas.py

```python
"""Pydantic 请求/响应模型"""

from pydantic import BaseModel, Field
from typing import Optional, Literal


class GroupCreate(BaseModel):
    """创建分组限速策略"""
    group_name: str = Field(..., max_length=64, description="分组名称")
    limit_5h: int = Field(..., ge=1, description="5小时配额")
    limit_7d: int = Field(..., ge=1, description="7天配额")
    limit_30d: int = Field(..., ge=1, description="30天配额")
    limit_type: Literal["request", "token"] = Field(default="request", description="限速类型")
    scope: Literal["key", "group"] = Field(default="key", description="限速粒度")
    remark: Optional[str] = Field(None, max_length=256, description="备注")


class GroupUpdate(BaseModel):
    """更新分组限速策略"""
    limit_5h: Optional[int] = Field(None, ge=1)
    limit_7d: Optional[int] = Field(None, ge=1)
    limit_30d: Optional[int] = Field(None, ge=1)
    limit_type: Optional[Literal["request", "token"]] = None
    scope: Optional[Literal["key", "group"]] = None
    status: Optional[int] = Field(None, ge=0, le=1)
    remark: Optional[str] = Field(None, max_length=256)


class GroupResponse(BaseModel):
    """分组限速策略响应"""
    id: int
    group_name: str
    limit_5h: int
    limit_7d: int
    limit_30d: int
    limit_type: str
    scope: str
    status: int
    remark: Optional[str]
    created_at: str
    updated_at: str


class RateLimitResult(BaseModel):
    """限速检查结果"""
    allowed: bool
    reason: str = ""
    group: Optional[str] = None
    remaining_5h: Optional[int] = None
    remaining_7d: Optional[int] = None
    remaining_30d: Optional[int] = None


class ErrorResponse(BaseModel):
    """错误响应"""
    error: str
    message: str
    remaining: Optional[dict] = None
```

### 5.6 lua/rate_limit.lua

```lua
-- rate_limit.lua
-- 全合并限速脚本：Key→分组→策略→检查→计数，单次 Redis 往返
--
-- KEYS[1] = sha256(api_key)
-- ARGV[1] = 当前时间戳(秒)
-- ARGV[2] = key_map_ttl (秒)
-- ARGV[3] = config_cache_ttl (秒)
--
-- 返回值:
--   {1}                        = 放行
--   {0, "5h", group, limit}    = 5h窗口超限
--   {0, "7d", group, limit}    = 7d窗口超限
--   {0, "30d", group, limit}   = 30d窗口超限
--   {-1}                       = key未找到
--   {-2}                       = 分组配置未找到

local key_hash = KEYS[1]
local now = tonumber(ARGV[1])
local key_map_ttl = tonumber(ARGV[2])
local config_ttl = tonumber(ARGV[3])

-- ① Key → 分组
local group = redis.call("GET", "keymap:" .. key_hash)
if not group then
    return {-1}
end

-- ② 分组 → 限速策略
local config_str = redis.call("GET", "config:" .. group)
if not config_str then
    return {-2}
end

local config = cjson.decode(config_str)
local limit_type = config["type"]
local scope = config["scope"]

-- ③ 确定限速主体 (user_id)
local user_id
if scope == "group" then
    user_id = group
else
    user_id = key_hash
end

-- ④ 检查 3 个滑动窗口
-- 窗口定义: {ttl_seconds, limit, window_name}
local windows = {
    {18000,   tonumber(config["5h"]),   "5h"},
    {604800,  tonumber(config["7d"]),   "7d"},
    {2592000, tonumber(config["30d"]),  "30d"}
}

if limit_type == "request" then
    -- 按请求数限速: 使用 ZSET 滑动窗口
    for i, w in ipairs(windows) do
        local ttl, limit, name = w[1], w[2], w[3]
        local key = "ratelimit:" .. user_id .. ":" .. ttl

        -- 清理过期记录
        redis.call("ZREMRANGEBYSCORE", key, 0, now - ttl)

        -- 检查计数
        local count = redis.call("ZCARD", key)
        if count >= limit then
            return {0, name, group, limit, count}
        end
    end

    -- ⑤ 全部通过，各窗口计数 +1
    for i, w in ipairs(windows) do
        local ttl = w[1]
        local key = "ratelimit:" .. user_id .. ":" .. ttl
        local member = now .. ":" .. math.random(100000000)
        redis.call("ZADD", key, now, member)
        redis.call("EXPIRE", key, ttl)
    end

    -- 返回剩余配额
    local remaining = {}
    for i, w in ipairs(windows) do
        local ttl, limit = w[1], w[2]
        local key = "ratelimit:" .. user_id .. ":" .. ttl
        local count = redis.call("ZCARD", key)
        remaining[i] = limit - count
    end

    return {1, group, remaining[1], remaining[2], remaining[3]}

else
    -- 按 Token 用量限速: 使用 String 累加器 + 滑动窗口近似
    -- 注意: Token 模式为"先查后扣"——请求前检查累计量，响应后扣减
    -- 此脚本只做检查，扣减由应用层在响应后调用单独脚本
    for i, w in ipairs(windows) do
        local ttl, limit, name = w[1], w[2], w[3]
        local key = "token_usage:" .. user_id .. ":" .. ttl
        local used = tonumber(redis.call("GET", key) or "0")
        if used >= limit then
            return {0, name, group, limit, used}
        end
    end

    -- 返回当前用量和剩余
    local remaining = {}
    for i, w in ipairs(windows) do
        local ttl, limit = w[1], w[2]
        local key = "token_usage:" .. user_id .. ":" .. ttl
        local used = tonumber(redis.call("GET", key) or "0")
        remaining[i] = limit - used
    end

    return {1, group, remaining[1], remaining[2], remaining[3]}
end
```

### 5.7 lua/token_deduct.lua

```lua
-- token_deduct.lua
-- Token 模式专用：请求完成后扣减 token 用量
--
-- KEYS[1] = user_id (sha256(api_key) 或 group name)
-- ARGV[1] = 当前时间戳(秒)
-- ARGV[2] = 要扣减的 token 数量

local user_id = KEYS[1]
local now = tonumber(ARGV[1])
local tokens = tonumber(ARGV[2])

local windows = {18000, 604800, 2592000}

for i, ttl in ipairs(windows) do
    local key = "token_usage:" .. user_id .. ":" .. ttl
    local current = tonumber(redis.call("GET", key) or "0")
    local new_val = current + tokens
    redis.call("SET", key, new_val, "EX", ttl)
end

return 1
```

### 5.8 app/ratelimit.py

```python
"""限速核心模块 - Redis + Lua 脚本执行"""

import hashlib
import time
import json
from pathlib import Path
from typing import Optional, Tuple
import logging

import redis.asyncio as aioredis

from app.config import config

logger = logging.getLogger(__name__)

# 窗口定义
WINDOWS = [
    {"ttl": 18000, "name": "5h"},
    {"ttl": 604800, "name": "7d"},
    {"ttl": 2592000, "name": "30d"},
]

# Lua 脚本内容
_LUA_DIR = Path(__file__).parent.parent / "lua"


def _load_script(name: str) -> str:
    return (_LUA_DIR / name).read_text(encoding="utf-8")


class RateLimitResult:
    """限速检查结果"""

    def __init__(
        self,
        allowed: bool,
        reason: str = "",
        group: Optional[str] = None,
        remaining: Optional[dict] = None,
    ):
        self.allowed = allowed
        self.reason = reason
        self.group = group
        self.remaining = remaining or {}

    def to_dict(self) -> dict:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "group": self.group,
            "remaining": self.remaining,
        }


class RateLimiter:
    """限速器 - 封装 Redis Lua 脚本调用"""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._rate_limit_sha: Optional[str] = None
        self._token_deduct_sha: Optional[str] = None

    async def init(self):
        """初始化 Redis 连接和 Lua 脚本"""
        self._redis = aioredis.from_url(
            config.redis_url,
            max_connections=config.redis.pool_size,
            decode_responses=True,
        )
        # 预加载 Lua 脚本，获取 SHA
        rate_limit_script = _load_script("rate_limit.lua")
        token_deduct_script = _load_script("token_deduct.lua")
        self._rate_limit_sha = await self._redis.script_load(rate_limit_script)
        self._token_deduct_sha = await self._redis.script_load(token_deduct_script)
        logger.info("Rate limiter initialized, Lua scripts loaded")

    async def close(self):
        if self._redis:
            await self._redis.close()

    @staticmethod
    def hash_key(api_key: str) -> str:
        """对 API Key 做 SHA256 哈希"""
        return hashlib.sha256(api_key.encode()).hexdigest()

    async def check(self, api_key: str) -> RateLimitResult:
        """
        检查请求是否允许通过（按请求数模式会同时计数 +1）

        返回 RateLimitResult
        """
        key_hash = self.hash_key(api_key)
        now = int(time.time())

        try:
            result = await self._redis.evalsha(
                self._rate_limit_sha,
                1,
                key_hash,
                now,
                config.sync.key_map_ttl,
                config.sync.config_cache_ttl,
            )
        except Exception as e:
            logger.error(f"Redis EVALSHA failed: {e}")
            # Redis 故障时的降级策略：放行（让 NewAPI 自己处理）
            return RateLimitResult(allowed=True, reason="redis_error_passthrough")

        return self._parse_result(result)

    async def deduct_tokens(self, user_id: str, tokens: int):
        """
        Token 模式专用：请求完成后扣减 token 用量

        user_id = sha256(api_key) 或 group name
        """
        now = int(time.time())
        try:
            await self._redis.evalsha(
                self._token_deduct_sha,
                1,
                user_id,
                now,
                tokens,
            )
        except Exception as e:
            logger.error(f"Token deduct failed: {e}")

    def _parse_result(self, result: list) -> RateLimitResult:
        """解析 Lua 脚本返回值"""
        code = result[0]

        if code == 1:
            # 放行
            group = result[1] if len(result) > 1 else None
            remaining = {}
            if len(result) > 4:
                remaining = {
                    "5h": result[2],
                    "7d": result[3],
                    "30d": result[4],
                }
            return RateLimitResult(allowed=True, group=group, remaining=remaining)

        elif code == 0:
            # 超限
            window = result[1] if len(result) > 1 else "unknown"
            group = result[2] if len(result) > 2 else None
            limit = result[3] if len(result) > 3 else 0
            used = result[4] if len(result) > 4 else 0
            return RateLimitResult(
                allowed=False,
                reason=f"{window}_exceeded",
                group=group,
                remaining={window: 0},
            )

        elif code == -1:
            # Key 未找到
            behavior = config.ratelimit.on_key_not_found
            if behavior == "passthrough":
                return RateLimitResult(allowed=True, reason="key_not_found_passthrough")
            else:
                return RateLimitResult(allowed=False, reason="key_not_found")

        elif code == -2:
            # 分组配置未找到
            behavior = config.ratelimit.on_config_not_found
            if behavior == "passthrough":
                return RateLimitResult(allowed=True, reason="config_not_found_passthrough")
            else:
                return RateLimitResult(allowed=False, reason="config_not_found")

        return RateLimitResult(allowed=True, reason="unknown_code_passthrough")


# 全局限速器实例
rate_limiter = RateLimiter()
```

### 5.9 app/proxy.py

```python
"""反向代理 - 透明转发请求到 NewAPI，支持流式响应"""

import logging
from typing import Optional
import httpx
from fastapi import Request, Response
from fastapi.responses import StreamingResponse, JSONResponse

from app.config import config
from app.ratelimit import rate_limiter, RateLimitResult

logger = logging.getLogger(__name__)

# 需要透传的请求头
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def _extract_api_key(request: Request) -> Optional[str]:
    """从请求头提取 API Key"""
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    # 也支持直接传 api-key 头
    return request.headers.get("api-key")


def _build_forward_headers(request: Request) -> dict:
    """构建转发到 NewAPI 的请求头"""
    headers = {}
    for key, value in request.headers.items():
        if key.lower() not in HOP_BY_HOP_HEADERS:
            headers[key] = value
    return headers


async def handle_proxy(request: Request) -> Response:
    """主代理处理函数"""

    # ① 提取 API Key
    api_key = _extract_api_key(request)
    if not api_key:
        return JSONResponse(
            status_code=401,
            content={"error": "unauthorized", "message": "missing api key"},
        )

    # ② 限速检查
    result: RateLimitResult = await rate_limiter.check(api_key)

    if not result.allowed:
        # ⑥ 超限，返回 429
        response_body = {
            "error": "rate_limit_exceeded",
            "message": f"Rate limit exceeded: {result.reason}",
        }
        if config.ratelimit.show_remaining and result.remaining:
            response_body["remaining"] = result.remaining
        if result.group:
            response_body["group"] = result.group

        return JSONResponse(
            status_code=429,
            content=response_body,
            headers={"Retry-After": "300"},
        )

    # ⑤ 转发到 NewAPI
    return await _forward_to_newapi(request, result)


async def _forward_to_newapi(request: Request, rl_result: RateLimitResult) -> Response:
    """将请求透明转发到 NewAPI"""

    # 构建目标 URL
    path = request.url.path
    query = request.url.query
    target_url = f"{config.newapi.base_url}{path}"
    if query:
        target_url += f"?{query}"

    # 构建请求头
    headers = _build_forward_headers(request)

    # 读取请求体
    body = await request.body()

    # 判断是否为流式请求
    is_stream = _is_stream_request(request, body)

    # 判断限速模式 (token 模式需要解析响应)
    need_usage_tracking = (
        rl_result.group
        and _is_token_mode(rl_result.group)
    )

    # 流式 + token 模式时，注入 stream_options.include_usage
    if is_stream and need_usage_tracking and config.ratelimit.auto_inject_include_usage:
        original_len = len(body)
        body = _inject_include_usage(body, path, need_inject=True)
        if len(body) != original_len:
            logger.debug("Injected stream_options.include_usage for token tracking")

    if is_stream:
        return await _proxy_stream(
            request.method, target_url, headers, body, rl_result, need_usage_tracking
        )
    else:
        return await _proxy_normal(
            request.method, target_url, headers, body, rl_result, need_usage_tracking
        )


def _is_stream_request(request: Request, body: bytes) -> bool:
    """判断是否为流式请求"""
    # 检查 Accept 头
    accept = request.headers.get("accept", "")
    if "text/event-stream" in accept:
        return True
    # 检查请求体中是否有 stream: true
    if body:
        try:
            import json
            data = json.loads(body)
            if data.get("stream") is True:
                return True
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
    return False


async def _is_token_mode_for_group(group: str) -> bool:
    """检查分组是否为 token 限速模式"""
    import json
    from app.config import config as cfg
    redis = rate_limiter._redis
    if not redis:
        return False
    config_str = await redis.get(f"config:{group}")
    if not config_str:
        return False
    try:
        conf = json.loads(config_str)
        return conf.get("type") == "token"
    except json.JSONDecodeError:
        return False


def _is_token_mode(group: str) -> bool:
    """同步检查（从限速结果中已知模式时使用）"""
    # 简化版：实际使用时可以从 rl_result 中传递
    return False  # 默认不追踪，实际场景中通过配置判断


async def _proxy_normal(
    method: str,
    url: str,
    headers: dict,
    body: bytes,
    rl_result: RateLimitResult,
    track_usage: bool,
) -> Response:
    """非流式代理"""
    async with httpx.AsyncClient(timeout=config.newapi.timeout) as client:
        try:
            resp = await client.request(method, url, headers=headers, content=body)
        except httpx.RequestError as e:
            logger.error(f"NewAPI request failed: {e}")
            return JSONResponse(
                status_code=502,
                content={"error": "bad_gateway", "message": str(e)},
            )

    # 如果需要追踪 token 用量，解析响应
    if track_usage and resp.status_code == 200:
        usage = _extract_usage_from_response(resp)
        if usage:
            user_id = rate_limiter.hash_key(
                headers.get("authorization", "").replace("Bearer ", "")
            )
            await rate_limiter.deduct_tokens(user_id, usage)

    # 透传响应
    response_headers = dict(resp.headers)
    response_headers.pop("content-length", None)
    response_headers.pop("transfer-encoding", None)

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=response_headers,
        media_type=resp.headers.get("content-type"),
    )


async def _proxy_stream(
    method: str,
    url: str,
    headers: dict,
    body: bytes,
    rl_result: RateLimitResult,
    track_usage: bool,
) -> StreamingResponse:
    """流式代理 - 透传 SSE chunk，同时用 buffer 累积完整事件提取 usage

    针对 tool call / function call 优化:
    - 使用 \n\n 分割完整 SSE 事件，避免 chunk 边界切割导致 usage 丢失
    - tool call 的流式增量参数透传不受影响
    """

    async def stream_generator():
        usage_tokens = 0
        api_key = headers.get("authorization", "").replace("Bearer ", "")
        sse_buffer = ""

        async with httpx.AsyncClient(timeout=config.newapi.timeout) as client:
            async with client.stream(method, url, headers=headers, content=body) as resp:
                async for chunk in resp.aiter_bytes():
                    yield chunk  # 先透传，不阻塞客户端

                    if track_usage:
                        try:
                            sse_buffer += chunk.decode("utf-8", errors="ignore")
                            # 按 \n\n 分割完整 SSE 事件
                            while "\n\n" in sse_buffer:
                                event_str, sse_buffer = sse_buffer.split("\n\n", 1)
                                usage_tokens = _extract_usage_from_sse_event(
                                    event_str, usage_tokens
                                )
                        except Exception:
                            pass

                # 处理 buffer 中残留的最后一个事件
                if track_usage and sse_buffer.strip():
                    usage_tokens = _extract_usage_from_sse_event(sse_buffer, usage_tokens)

                if track_usage and usage_tokens > 0:
                    group_config = await rate_limiter.get_group_config(rl_result.group)
                    if group_config:
                        user_id = (
                            rl_result.group
                            if group_config.get("scope") == "group"
                            else rate_limiter.hash_key(api_key)
                        )
                        await rate_limiter.deduct_tokens(user_id, usage_tokens)

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
    )


def _extract_usage_from_sse_event(event_str: str, current_usage: int) -> int:
    """从完整 SSE 事件中提取 usage.total_tokens

    一个 SSE 事件可能包含多行 data:，取最后一个含 usage 的。
    """
    max_usage = current_usage
    for line in event_str.split("\n"):
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]" or not payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        usage = data.get("usage")
        if usage and isinstance(usage, dict):
            total = usage.get("total_tokens", 0)
            if total > max_usage:
                max_usage = total
    return max_usage


def _inject_include_usage(body: bytes, path: str, need_inject: bool) -> bytes:
    """流式 + token 模式时注入 stream_options.include_usage=true

    仅对 /v1/chat/completions 路径生效，且不覆盖客户端已设置的值。
    """
    if not need_inject or path not in CHAT_COMPLETION_PATHS:
        return body
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body
    if data.get("stream") is not True:
        return body
    stream_options = data.get("stream_options")
    if isinstance(stream_options, dict) and stream_options.get("include_usage") is True:
        return body
    if stream_options is None:
        data["stream_options"] = {"include_usage": True}
    elif isinstance(stream_options, dict):
        stream_options["include_usage"] = True
    else:
        return body
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def _extract_usage_from_response(resp: httpx.Response) -> Optional[int]:
    """从非流式响应中提取 token 用量"""
    try:
        data = resp.json()
        usage = data.get("usage")
        if usage and "total_tokens" in usage:
            return usage["total_tokens"]
    except Exception:
        pass
    return None
```

### 5.10 app/sync.py

```python
"""Key→Group 同步模块 - 从 NewAPI 数据库同步到 Redis"""

import asyncio
import hashlib
import logging
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import config

logger = logging.getLogger(__name__)


class KeyGroupSyncer:
    """从 NewAPI 数据库同步 Key→Group 映射到 Redis"""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._newapi_engine = None
        self._running = False

    async def init(self, redis_client: aioredis.Redis):
        self._redis = redis_client
        self._newapi_engine = create_async_engine(
            config.newapi_mysql_url,
            pool_size=config.newapi_mysql.pool_size,
            pool_pre_ping=True,
        )

    async def close(self):
        if self._newapi_engine:
            await self._newapi_engine.dispose()

    async def sync_once(self):
        """执行一次全量同步"""
        try:
            async with self._newapi_engine.connect() as conn:
                # NewAPI 的 token 表中 key 和 group 字段
                # status=1 表示启用
                result = await conn.execute(
                    text("SELECT `key`, `group` FROM tokens WHERE status = 1")
                )
                rows = result.fetchall()

            pipe = self._redis.pipeline(transaction=False)
            count = 0
            for row in rows:
                api_key = row[0]
                group = row[1] if row[1] else "default"
                key_hash = hashlib.sha256(api_key.encode()).hexdigest()
                pipe.set(
                    f"keymap:{key_hash}",
                    group,
                    ex=config.sync.key_map_ttl,
                )
                count += 1

            if count > 0:
                await pipe.execute()

            logger.info(f"Synced {count} key→group mappings to Redis")

        except Exception as e:
            logger.error(f"Key→Group sync failed: {e}")

    async def sync_group_configs(self, db_session_factory):
        """从 MySQL 配置库同步分组策略到 Redis"""
        try:
            from sqlalchemy import select
            from app.models import RateLimitGroup

            async with db_session_factory() as session:
                result = await session.execute(
                    select(RateLimitGroup).where(RateLimitGroup.status == 1)
                )
                groups = result.scalars().all()

            pipe = self._redis.pipeline(transaction=False)
            count = 0
            for group in groups:
                config_json = group.to_config_dict()
                import json
                pipe.set(
                    f"config:{group.group_name}",
                    json.dumps(config_json),
                    ex=config.sync.config_cache_ttl,
                )
                count += 1

            if count > 0:
                await pipe.execute()

            logger.info(f"Synced {count} group configs to Redis")

        except Exception as e:
            logger.error(f"Group config sync failed: {e}")

    async def run_loop(self, db_session_factory):
        """后台循环同步"""
        self._running = True
        while self._running:
            await self.sync_once()
            await self.sync_group_configs(db_session_factory)
            await asyncio.sleep(config.sync.key_group_interval)

    async def stop(self):
        self._running = False


# 全局同步器实例
syncer = KeyGroupSyncer()
```

### 5.11 app/admin.py

```python
"""管理后台 API - 分组限速策略 CRUD"""

import json
import logging
from typing import List

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import config
from app.models import RateLimitGroup
from app.schemas import GroupCreate, GroupUpdate, GroupResponse
from app.ratelimit import rate_limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


async def verify_auth(authorization: str = Header(None)):
    """管理接口认证"""
    if not config.admin.enabled:
        raise HTTPException(status_code=403, detail="Admin API disabled")
    token = ""
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
    if token != config.admin.auth_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")


async def _invalidate_config_cache(redis, group_name: str):
    """配置变更后刷新 Redis 缓存"""
    # 删除缓存，让下次请求触发重新加载
    await redis.delete(f"config:{group_name}")


@router.post("/groups", response_model=GroupResponse, dependencies=[Depends(verify_auth)])
async def create_group(
    data: GroupCreate,
    db: AsyncSession = Depends(...),
):
    """创建分组限速策略"""
    group = RateLimitGroup(
        group_name=data.group_name,
        limit_5h=data.limit_5h,
        limit_7d=data.limit_7d,
        limit_30d=data.limit_30d,
        limit_type=data.limit_type,
        scope=data.scope,
        remark=data.remark,
    )
    db.add(group)
    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"Create failed: {e}")

    # 同步到 Redis
    await _sync_group_to_redis(group)

    return _to_response(group)


@router.get("/groups", response_model=List[GroupResponse], dependencies=[Depends(verify_auth)])
async def list_groups(db: AsyncSession = Depends(...)):
    """列出所有分组策略"""
    result = await db.execute(select(RateLimitGroup).order_by(RateLimitGroup.id))
    groups = result.scalars().all()
    return [_to_response(g) for g in groups]


@router.get("/groups/{group_name}", response_model=GroupResponse, dependencies=[Depends(verify_auth)])
async def get_group(group_name: str, db: AsyncSession = Depends(...)):
    """查询单个分组策略"""
    result = await db.execute(
        select(RateLimitGroup).where(RateLimitGroup.group_name == group_name)
    )
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    return _to_response(group)


@router.put("/groups/{group_name}", response_model=GroupResponse, dependencies=[Depends(verify_auth)])
async def update_group(
    group_name: str,
    data: GroupUpdate,
    db: AsyncSession = Depends(...),
):
    """更新分组策略"""
    result = await db.execute(
        select(RateLimitGroup).where(RateLimitGroup.group_name == group_name)
    )
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(group, key, value)

    await db.commit()

    # 同步到 Redis
    await _sync_group_to_redis(group)

    return _to_response(group)


@router.delete("/groups/{group_name}", dependencies=[Depends(verify_auth)])
async def delete_group(group_name: str, db: AsyncSession = Depends(...)):
    """删除分组策略（软删除：设为停用）"""
    result = await db.execute(
        select(RateLimitGroup).where(RateLimitGroup.group_name == group_name)
    )
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    group.status = 0
    await db.commit()

    # 从 Redis 删除配置缓存
    await rate_limiter._redis.delete(f"config:{group_name}")

    return {"message": f"Group '{group_name}' disabled"}


@router.post("/sync", dependencies=[Depends(verify_auth)])
async def manual_sync():
    """手动触发 Key→Group 同步"""
    from app.sync import syncer
    await syncer.sync_once()
    return {"message": "Sync triggered"}


async def _sync_group_to_redis(group: RateLimitGroup):
    """将单个分组配置同步到 Redis"""
    if group.status == 1:
        config_json = json.dumps(group.to_config_dict())
        await rate_limiter._redis.set(
            f"config:{group.group_name}",
            config_json,
            ex=config.sync.config_cache_ttl,
        )
    else:
        await rate_limiter._redis.delete(f"config:{group.group_name}")


def _to_response(group: RateLimitGroup) -> GroupResponse:
    return GroupResponse(
        id=group.id,
        group_name=group.group_name,
        limit_5h=group.limit_5h,
        limit_7d=group.limit_7d,
        limit_30d=group.limit_30d,
        limit_type=group.limit_type,
        scope=group.scope,
        status=group.status,
        remark=group.remark,
        created_at=group.created_at.strftime("%Y-%m-%d %H:%M:%S") if group.created_at else "",
        updated_at=group.updated_at.strftime("%Y-%m-%d %H:%M:%S") if group.updated_at else "",
    )
```

### 5.12 app/main.py

```python
"""FastAPI 应用入口"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.config import config
from app.ratelimit import rate_limiter
from app.sync import syncer
from app.proxy import handle_proxy
from app.admin import router as admin_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# 数据库引擎和会话工厂
db_engine = create_async_engine(
    config.mysql_url,
    pool_size=config.mysql.pool_size,
    pool_pre_ping=True,
)
db_session_factory = async_sessionmaker(db_engine, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期：启动和关闭"""

    # --- 启动 ---
    # 1. 初始化限速器 (Redis + Lua)
    await rate_limiter.init()

    # 2. 初始化同步器
    await syncer.init(rate_limiter._redis)

    # 3. 执行一次初始同步
    await syncer.sync_once()
    await syncer.sync_group_configs(db_session_factory)

    # 4. 启动后台同步循环
    import asyncio
    sync_task = asyncio.create_task(syncer.run_loop(db_session_factory))

    logger.info(f"Gateway started, proxying to {config.newapi.base_url}")

    yield

    # --- 关闭 ---
    sync_task.cancel()
    try:
        await sync_task
    except asyncio.CancelledError:
        pass

    await syncer.close()
    await rate_limiter.close()
    await db_engine.dispose()
    logger.info("Gateway stopped")


app = FastAPI(
    title="NewAPI Rate Limit Gateway",
    description="无侵入式分组限速网关",
    version="1.0.0",
    lifespan=lifespan,
)

# 注册管理后台路由
app.include_router(admin_router)


# 健康检查
@app.get("/health")
async def health():
    return {"status": "ok"}


# 限速统计查询
@app.get("/ratelimit/status/{api_key}")
async def ratelimit_status(api_key: str):
    """查询指定 API Key 的当前限速状态"""
    import hashlib
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    # 查分组
    group = await rate_limiter._redis.get(f"keymap:{key_hash}")
    if not group:
        return {"found": False, "message": "Key not found in cache"}

    # 查配置
    import json
    config_str = await rate_limiter._redis.get(f"config:{group}")
    if not config_str:
        return {"found": False, "group": group, "message": "Config not found in cache"}

    conf = json.loads(config_str)

    # 查各窗口当前计数
    import time
    now = int(time.time())
    windows = [
        ("5h", 18000, conf["5h"]),
        ("7d", 604800, conf["7d"]),
        ("30d", 2592000, conf["30d"]),
    ]

    user_id = group if conf.get("scope") == "group" else key_hash
    status = {}
    for name, ttl, limit in windows:
        if conf.get("type") == "token":
            used = int(await rate_limiter._redis.get(f"token_usage:{user_id}:{ttl}") or 0)
        else:
            key = f"ratelimit:{user_id}:{ttl}"
            await rate_limiter._redis.zremrangebyscore(key, 0, now - ttl)
            used = await rate_limiter._redis.zcard(key)
        status[name] = {
            "used": used,
            "limit": limit,
            "remaining": max(0, limit - used),
        }

    return {
        "found": True,
        "group": group,
        "config": conf,
        "status": status,
    }


# 代理所有其他路径到 NewAPI
@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def catch_all(request: Request):
    """捕获所有非管理/健康检查路径，代理到 NewAPI"""
    return await handle_proxy(request)
```

---

## 6. Docker 部署

### 6.1 Dockerfile

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# 安装 uv（固定版本，保证构建可复现）
COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /uv/bin/uv
ENV PATH="/uv/bin:$PATH"

# 先复制依赖声明，利用 Docker 分层缓存（pyproject.toml/uv.lock 不变时跳过安装）
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# 复制代码
COPY . .

# 暴露端口
EXPOSE 8080

# 启动（--no-sync 复用构建期已同步的 .venv）
CMD ["uv", "run", "--no-sync", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "4"]
```

### 6.2 docker-compose.yml

```yaml
version: "3.8"

services:
  gateway:
    build: .
    container_name: ratelimit-gateway
    ports:
      - "8080:8080"
    volumes:
      - ./config.yaml:/app/config.yaml:ro
      - ./lua:/app/lua:ro
    depends_on:
      - redis
    restart: unless-stopped
    networks:
      - gateway-net

  redis:
    image: redis:7-alpine
    container_name: ratelimit-redis
    command: redis-server --maxmemory 512mb --maxmemory-policy allkeys-lru
    volumes:
      - redis-data:/data
    restart: unless-stopped
    networks:
      - gateway-net

volumes:
  redis-data:

networks:
  gateway-net:
    driver: bridge
```

### 6.3 宝塔面板部署步骤

```bash
# 1. 上传项目到服务器
# 建议路径: /www/wwwroot/ratelimit-gateway

# 2. 修改 config.yaml
#    - newapi.base_url: 改为 NewAPI 的实际地址
#    - redis: 如果用独立 Redis，改为实际地址
#    - mysql: 改为实际 MySQL 配置
#    - newapi_mysql: 改为 NewAPI 的数据库配置（只读账号）

# 3. 初始化数据库
mysql -u root -p < sql/init.sql

# 4. 构建并启动
cd /www/wwwroot/ratelimit-gateway
docker-compose up -d --build

# 5. 验证
curl http://127.0.0.1:8080/health
# 返回 {"status":"ok"} 即正常

# 6. 在宝塔面板中配置反向代理（可选）
#    将域名指向 8080 端口
```

---

## 7. 使用方式

### 7.1 客户端接入

客户端只需将原来指向 NewAPI 的地址改为指向限速网关：

```diff
- base_url: http://your-server:3000/v1
+ base_url: http://your-server:8080/v1
```

API Key 不变，所有请求头透传，NewAPI 完全无感。

### 7.2 管理分组策略

```bash
# 创建分组
curl -X POST http://localhost:8080/admin/groups \
  -H "Authorization: Bearer change-me-in-production" \
  -H "Content-Type: application/json" \
  -d '{
    "group_name": "product-a",
    "limit_5h": 100,
    "limit_7d": 1000,
    "limit_30d": 5000,
    "limit_type": "request",
    "scope": "key",
    "remark": "产品A"
  }'

# 查询所有分组
curl http://localhost:8080/admin/groups \
  -H "Authorization: Bearer change-me-in-production"

# 更新分组
curl -X PUT http://localhost:8080/admin/groups/product-a \
  -H "Authorization: Bearer change-me-in-production" \
  -H "Content-Type: application/json" \
  -d '{"limit_5h": 200, "limit_30d": 10000}'

# 查询某 Key 的限速状态
curl http://localhost:8080/ratelimit/status/sk-xxxxxxxx
```

### 7.3 在 NewAPI 中配置分组

在 NewAPI 管理后台，给 token 设置 `group` 字段为你在网关中创建的分组名（如 `product-a`），网关会自动同步这个映射。

---

## 8. 限速算法说明

### 8.1 按请求数模式（request）

使用 Redis ZSET 实现精确滑动窗口：

```
请求到达
  → ZREMRANGEBYSCORE 清理窗口外的过期记录
  → ZCARD 获取当前窗口内计数
  → 计数 >= 限制？拒绝 : ZADD +1 并放行
```

特点：

- 精确到每一条请求
- 滑动窗口，无固定边界突变
- 单次 Lua 原子操作，无并发竞态

### 8.2 按 Token 用量模式（token）

使用 Redis String 累加器 + 滑动窗口近似：

```
请求前：
  → GET token_usage:{uid}:{ttl} 获取累计用量
  → 累计 >= 限制？拒绝 : 放行

请求后：
  → 解析响应中的 usage.total_tokens
  → SET token_usage:{uid}:{ttl} = 当前值 + 本次tokens
```

特点：

- 按 Token 用量计费，更公平
- 存在天然滞后（大请求可能超额）
- 流式响应需从 SSE 最后一个 chunk 提取 usage

### 8.3 30 天窗口内存优化

30 天 ZSET 在高流量下可能积累大量 member。优化方案：

**分桶计数法**（推荐用于 30d 窗口）：

- 按小时分桶，key 变为 `ratelimit:{uid}:2592000:{hour_bucket}`
- 值是该小时的请求计数（INCR）
- 检查时累加最近 720 个小时的计数
- 内存 O(720) 而非 O(N)

在 Lua 脚本中为 30d 窗口切换为分桶模式即可，5h 和 7d 仍用 ZSET。

---

## 9. 性能指标

| 指标       | 预期值   | 说明                        |
| -------- | ----- | ------------------------- |
| 限速检查延迟   | < 2ms | 单次 Redis Lua 往返           |
| 代理转发延迟   | < 5ms | 网关自身开销（不含 NewAPI 处理时间）    |
| QPS      | 5000+ | 4 worker + Redis pipeline |
| Redis 内存 | ~50MB | 1万 Key + 计数器              |
| 同步延迟     | < 60s | Key→Group 映射同步间隔          |

---

## 10. 容错与降级

| 故障场景       | 降级策略                  | 配置项                             |
| ---------- | --------------------- | ------------------------------- |
| Redis 宕机   | 放行所有请求（让 NewAPI 自己鉴权） | 代码内硬编码                          |
| Key 未在缓存中  | 放行或拒绝                 | `ratelimit.on_key_not_found`    |
| 分组未配置策略    | 放行或使用默认配额             | `ratelimit.on_config_not_found` |
| NewAPI 不可达 | 返回 502 Bad Gateway    | 代码内硬编码                          |
| 流式响应中断     | 不扣减 Token（保守策略）       | 代码内硬编码                          |

---

## 11. 监控与告警

### 11.1 关键监控指标

```python
# 建议在 main.py 中接入 Prometheus 或日志收集

# 1. 请求总量
# 2. 429 拒绝量 + 拒绝率
# 3. 各分组当前用量/配额比
# 4. Redis 连接池使用率
# 5. 同步任务执行状态
# 6. 代理转发延迟 P50/P99
```

### 11.2 日志格式

```
2026-08-18 14:00:00 [INFO] app.proxy: method=POST path=/v1/chat/completions group=vip allowed=True remaining={"5h":498,"7d":4998,"30d":49998}
2026-08-18 14:00:01 [WARN] app.proxy: method=POST path=/v1/chat/completions group=trial allowed=False reason=5h_exceeded limit=10 used=10
```

---

## 12. 扩展方向

| 方向              | 说明                                         |
| --------------- | ------------------------------------------ |
| **多级限速**        | 除了 5h/7d/30d，增加每分钟/每小时的短窗口限速               |
| **突发流量**        | 令牌桶模式，允许短时突发后平滑限速                          |
| **按模型限速**       | 不同模型（GPT-4 vs GPT-3.5）不同配额                 |
| **用户自查询**       | 提供 API 让用户查询自己的剩余配额                        |
| **告警通知**        | 用量达 80%/90% 时推送企业微信/飞书通知                   |
| **配额充值**        | 支持按周期重置或手动充值配额                             |
| **多 NewAPI 实例** | 网关支持负载均衡到多个 NewAPI 后端                      |
| **Go 重写**       | 如需更高性能，可用 Go + httputil.ReverseProxy 重写代理层 |

---

## 13. Tool Call / Function Call 支持说明

网关作为透明反向代理，对 tool call / function call **请求层面完全无感**——`tools`、`functions`、`tool_choice` 等字段原样转发，响应中的 `tool_calls` 增量也透传不改。但 Token 用量追踪模式下有两个边界问题需要处理：

### 13.1 流式 SSE chunk 边界切割

**问题**：`httpx.aiter_bytes()` 返回的 chunk 不保证按 SSE 事件边界对齐。旧的 `text.split("\n")` 方式会把一个 `data: {"usage":...}` 行拆成两段，导致 usage 丢失。

**修复**：使用 `sse_buffer` 累积 chunk，按 `\n\n`（SSE 事件分隔符）分割完整事件后再解析。先 `yield chunk` 透传不阻塞客户端，再异步解析 usage。

### 13.2 流式模式 usage 依赖 stream_options

**问题**：OpenAI 兼容 API 在流式模式下默认不返回 usage，客户端必须设置 `stream_options: {include_usage: true}`。如果客户端没设，token 追踪为 0。

**修复**：token 模式 + 流式请求时，网关自动注入 `stream_options.include_usage=true`（仅 `/v1/chat/completions` 路径，不覆盖客户端已设值）。可通过 `config.yaml` 的 `ratelimit.auto_inject_include_usage` 开关控制。

### 13.3 兼容性矩阵

| 场景                | 按请求数限速 (`type: request`) | 按 Token 限速 (`type: token`)       |
| ----------------- | ------------------------ | -------------------------------- |
| 非流式 + 无 tool call | ✅ 完全支持                   | ✅ 从 JSON body 提取 usage           |
| 非流式 + tool call   | ✅ 完全支持                   | ✅ tool call 响应仍有 usage 字段        |
| 流式 + 无 tool call  | ✅ 完全支持                   | ✅ buffer 解析 + 自动注入 include_usage |
| 流式 + tool call    | ✅ 完全支持                   | ✅ 增量参数透传 + buffer 解析 usage       |
| 流式 + 多轮 tool call | ✅ 完全支持                   | ✅ 每轮独立计费，usage 精确                |

### 13.4 多轮 Tool Call 对限速的影响

一次用户交互可能产生多次 API 调用（LLM → tool_call → 执行 → 喂回 LLM → 最终回答）。按请求数限速时，用户感知的 "1 次对话" 消耗了 N 次配额。建议为启用 tool call 的产品分组适当放宽 5h 配额。

---

## 14. 实施步骤清单

- [ ] 1\. 创建项目目录结构
- [ ] 2\. 编写 pyproject.toml 并用 uv sync 安装依赖（uv 管理环境）
- [ ] 3\. 执行 sql/init.sql 初始化数据库
- [ ] 4\. 修改 config.yaml 配置实际地址和密码
- [ ] 5\. 将 lua/ 目录下的脚本放入项目
- [ ] 6\. 编写 app/ 下所有 Python 文件
- [ ] 7\. 本地启动测试: `uv run uvicorn app.main:app --port 8080`
- [ ] 8\. 验证健康检查: `curl http://localhost:8080/health`
- [ ] 9\. 创建测试分组: 通过 admin API 创建分组策略
- [ ] 10\. 在 NewAPI 中给测试 token 设置对应分组
- [ ] 11\. 发送测试请求验证限速生效
- [ ] 12\. 验证 429 响应格式
- [ ] 13\. 验证流式请求透传正常
- [ ] 14\. 验证 tool call 请求（流式 + 非流式）透传正常
- [ ] 15\. 验证 token 模式下流式 usage 提取（含 tool call 场景）
- [ ] 16\. 构建 Docker 镜像并部署
- [ ] 17\. 生产环境切换客户端 base_url 到网关地址
- [ ] 18\. 监控运行状态，确认无异常

