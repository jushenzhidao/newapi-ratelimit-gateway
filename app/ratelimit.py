"""限速核心模块 - Redis + Lua 脚本执行

Redis 故障时的行为由 RATELIMIT_ON_REDIS_ERROR 控制：
- passthrough   : 放行请求（可用性优先，限速失效）
- reject        : 拒绝请求，返回 503（保护上游优先）
- local_fallback: 进程内滑动窗口兜底限速（粗粒度，多 worker 各自独立计数）

NOSCRIPT 自愈：Redis 重启后 Lua 脚本缓存丢失，EVALSHA 返回 NOSCRIPT，
此处自动重新 script_load 并重试一次，避免限速永久失效。
"""

import hashlib
import time
import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Optional
import asyncio
import logging

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from app.config import config

logger = logging.getLogger(__name__)

_LUA_DIR = Path(__file__).parent.parent / "lua"


def _load_script(name: str) -> str:
    return (_LUA_DIR / name).read_text(encoding="utf-8")


def _is_noscript(e: Exception) -> bool:
    return isinstance(e, ResponseError) and "NOSCRIPT" in str(e)


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
        # local_fallback 滑动窗口: key_hash -> deque[timestamp]
        self._fallback_windows: dict = defaultdict(deque)
        self._fallback_lock = asyncio.Lock()
        # 运行统计（进程生命周期内累计，供 /health 与告警使用）
        self._stats = {
            "redis_error_total": 0,
            "degrade_passthrough_total": 0,
            "degrade_reject_total": 0,
            "fallback_allow_total": 0,
            "fallback_reject_total": 0,
            "last_redis_error": "",
            "last_redis_error_at": 0.0,
        }

    async def init(self):
        """初始化 Redis 连接和 Lua 脚本"""
        self._redis = aioredis.from_url(
            config.redis_url,
            max_connections=config.redis.pool_size,
            decode_responses=True,
        )
        await self._load_scripts()
        logger.info("Rate limiter initialized, Lua scripts loaded")

    async def _load_scripts(self):
        """加载/重新加载 Lua 脚本（Redis 重启后脚本缓存丢失时调用）"""
        self._rate_limit_sha = await self._redis.script_load(
            _load_script("rate_limit.lua")
        )
        self._token_deduct_sha = await self._redis.script_load(
            _load_script("token_deduct.lua")
        )

    async def close(self):
        if self._redis:
            await self._redis.aclose()

    @staticmethod
    def hash_key(api_key: str) -> str:
        """对 API Key 做 SHA256 哈希"""
        return hashlib.sha256(api_key.encode()).hexdigest()

    async def check(self, api_key: str) -> RateLimitResult:
        """检查请求是否允许通过（按请求数模式同时计数 +1）

        Redis 异常时按 RATELIMIT_ON_REDIS_ERROR 策略降级；
        NOSCRIPT 时自动重载脚本并重试一次。
        """
        key_hash = self.hash_key(api_key)
        now = int(time.time())

        result = None
        for attempt in (1, 2):
            try:
                result = await self._redis.evalsha(
                    self._rate_limit_sha,
                    1,
                    key_hash,
                    now,
                    config.sync.key_map_ttl,
                    config.sync.config_cache_ttl,
                )
                break
            except Exception as e:
                if _is_noscript(e) and attempt == 1:
                    logger.warning("NOSCRIPT detected (Redis restarted?), reloading Lua scripts")
                    try:
                        await self._load_scripts()
                        continue  # 重试一次
                    except Exception as reload_err:
                        logger.error(f"Lua script reload failed: {reload_err}")
                return await self._degrade(api_key, e)

        return self._parse_result(result)

    async def deduct_tokens(self, user_id: str, tokens: int):
        """Token 模式专用：请求完成后扣减 token 用量"""
        now = int(time.time())
        for attempt in (1, 2):
            try:
                await self._redis.evalsha(
                    self._token_deduct_sha,
                    1,
                    user_id,
                    now,
                    tokens,
                )
                return
            except Exception as e:
                if _is_noscript(e) and attempt == 1:
                    logger.warning("NOSCRIPT detected in deduct_tokens, reloading")
                    try:
                        await self._load_scripts()
                        continue
                    except Exception as reload_err:
                        logger.error(f"Lua script reload failed: {reload_err}")
                logger.error(f"Token deduct failed: {e}")
                return

    async def get_group_config(self, group: str) -> Optional[dict]:
        """获取分组的限速配置"""
        try:
            config_str = await self._redis.get(f"config:{group}")
        except Exception as e:
            logger.error(f"Get group config failed: {e}")
            return None
        if not config_str:
            return None
        return json.loads(config_str)

    # ---------- Redis 故障降级 ----------

    async def _degrade(self, api_key: str, err: Exception) -> RateLimitResult:
        """Redis 异常时按配置策略降级"""
        self._stats["redis_error_total"] += 1
        self._stats["last_redis_error"] = f"{type(err).__name__}: {err}"[:300]
        self._stats["last_redis_error_at"] = time.time()
        logger.error(f"Redis unavailable, degrade mode={config.ratelimit.on_redis_error}: {err}")

        mode = config.ratelimit.on_redis_error
        if mode == "reject":
            self._stats["degrade_reject_total"] += 1
            return RateLimitResult(allowed=False, reason="redis_unavailable")

        if mode == "local_fallback":
            allowed = await self._local_fallback_allow(api_key)
            if allowed:
                self._stats["fallback_allow_total"] += 1
                return RateLimitResult(allowed=True, reason="redis_error_local_fallback")
            self._stats["fallback_reject_total"] += 1
            return RateLimitResult(allowed=False, reason="local_fallback_limit")

        # passthrough（默认）
        self._stats["degrade_passthrough_total"] += 1
        return RateLimitResult(allowed=True, reason="redis_error_passthrough")

    async def _local_fallback_allow(self, api_key: str) -> bool:
        """进程内滑动窗口兜底限速（每个 worker 独立计数，阈值应保守设置）"""
        window = config.ratelimit.fallback_window
        max_reqs = config.ratelimit.fallback_max_requests
        key_hash = self.hash_key(api_key)
        now = time.monotonic()

        async with self._fallback_lock:
            dq = self._fallback_windows[key_hash]
            cutoff = now - window
            while dq and dq[0] <= cutoff:
                dq.popleft()
            if len(dq) >= max_reqs:
                return False
            dq.append(now)

            # 防止 key 无限增长：超量时清理过期条目
            if len(self._fallback_windows) > 100_000:
                expired = [
                    k for k, v in self._fallback_windows.items()
                    if not v or v[-1] <= cutoff
                ]
                for k in expired:
                    del self._fallback_windows[k]

            return True

    # ---------- 可观测性 ----------

    async def ping(self) -> bool:
        """Redis 连通性检查（供 /health 使用）"""
        if not self._redis:
            return False
        try:
            return bool(await asyncio.wait_for(self._redis.ping(), timeout=2.0))
        except Exception:
            return False

    def stats(self) -> dict:
        """降级运行统计快照"""
        return dict(self._stats)

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
