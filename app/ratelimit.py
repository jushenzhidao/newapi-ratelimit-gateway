"""限速核心模块 - Redis + Lua 脚本执行"""

import hashlib
import time
import json
from pathlib import Path
from typing import Optional
import logging

import redis.asyncio as aioredis

from app.config import config

logger = logging.getLogger(__name__)

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
        rate_limit_script = _load_script("rate_limit.lua")
        token_deduct_script = _load_script("token_deduct.lua")
        self._rate_limit_sha = await self._redis.script_load(rate_limit_script)
        self._token_deduct_sha = await self._redis.script_load(token_deduct_script)
        logger.info("Rate limiter initialized, Lua scripts loaded")

    async def close(self):
        if self._redis:
            await self._redis.aclose()

    @staticmethod
    def hash_key(api_key: str) -> str:
        """对 API Key 做 SHA256 哈希"""
        return hashlib.sha256(api_key.encode()).hexdigest()

    async def check(self, api_key: str) -> RateLimitResult:
        """检查请求是否允许通过（按请求数模式同时计数 +1）"""
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
            return RateLimitResult(allowed=True, reason="redis_error_passthrough")

        return self._parse_result(result)

    async def deduct_tokens(self, user_id: str, tokens: int):
        """Token 模式专用：请求完成后扣减 token 用量"""
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

    async def get_group_config(self, group: str) -> Optional[dict]:
        """获取分组的限速配置"""
        config_str = await self._redis.get(f"config:{group}")
        if not config_str:
            return None
        return json.loads(config_str)

    def _parse_result(self, result: list) -> RateLimitResult:
        """解析 Lua 脚本返回值"""
        code = result[0]

        if code == 1:
            group = result[1] if len(result) > 1 else None
            remaining = {}
            if len(result) > 4:
                remaining = {"5h": result[2], "7d": result[3], "30d": result[4]}
            return RateLimitResult(allowed=True, group=group, remaining=remaining)

        elif code == 0:
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
            if config.ratelimit.on_key_not_found == "passthrough":
                return RateLimitResult(allowed=True, reason="key_not_found_passthrough")
            return RateLimitResult(allowed=False, reason="key_not_found")

        elif code == -2:
            if config.ratelimit.on_config_not_found == "passthrough":
                return RateLimitResult(allowed=True, reason="config_not_found_passthrough")
            return RateLimitResult(allowed=False, reason="config_not_found")

        return RateLimitResult(allowed=True, reason="unknown_passthrough")


rate_limiter = RateLimiter()
