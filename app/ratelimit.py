"""限速核心模块 - 三级解析 + 批量配额预取 + 熔断降级

架构（高可用重构后）:

1. keymap/config 解析在应用侧完成（app/resolver.py）:
   L1 进程内缓存 → Redis → MySQL 读透，Redis 挂掉时分组判断依然准确。

2. 请求数模式使用批量配额预取:
   每 worker 一次 EVALSHA 从 Redis 预取 N 个配额在本地消耗（flush 上次
   本地计数 + 检查 + 预取合并为单次往返）。Redis QPS 降为 1/N，且短暂
   抖动期间（N 个请求内）限速完全不感知。多 worker 并发预取的超发上界
   约为 (workers-1) * batch_size。

3. 熔断器（app/resilience.py）:
   Redis 连续失败达阈值后熔断打开，期间请求立即走降级路径，不再逐请求
   等待 Redis 超时，避免延迟雪崩；半开探测自动恢复。

4. Redis 故障降级由 RATELIMIT_ON_REDIS_ERROR 控制:
   - passthrough   : 放行（可用性优先）
   - reject        : 503 拒绝（保护上游优先）
   - local_fallback: 进程内滑动窗口兜底；已解析出分组策略时按
     「5h 限额 ÷ worker 数」执行分组感知兜底，否则用全局兜底参数。

5. NOSCRIPT 自愈: Redis 重启后自动重载 Lua 脚本并重试。
"""

import hashlib
import time
import json
import logging
from collections import defaultdict, deque
from pathlib import Path
from typing import Optional
import asyncio

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from app.config import config
from app.resilience import CircuitBreaker, CircuitOpenError

logger = logging.getLogger(__name__)

_LUA_DIR = Path(__file__).parent.parent / "lua"

# 本地滑动窗口兜底/配额状态的 key 数量上限（防内存无限增长）
_MAX_TRACKED_KEYS = 100_000
# 单次 flush 到 Redis 的最大时间戳数
_MAX_FLUSH_BATCH = 1000


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


class _QuotaState:
    """请求数模式的本地配额状态（每 worker 独立）"""

    __slots__ = ("deque", "reserved", "remaining", "last_used")

    def __init__(self):
        self.deque = deque()      # 已本地计数、待 flush 的请求时间戳（5h 保留）
        self.reserved = 0         # 本地剩余预取配额
        self.remaining = {}       # 最近一次预取返回的各窗口剩余
        self.last_used = time.monotonic()

    def trim(self, window: int, now: int):
        cutoff = now - window
        while self.deque and self.deque[0] <= cutoff:
            self.deque.popleft()


class RateLimiter:
    """限速器 - 解析 / 批量配额 / 熔断 / 降级"""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._request_sha: Optional[str] = None
        self._token_check_sha: Optional[str] = None
        self._token_deduct_sha: Optional[str] = None
        self._breaker = CircuitBreaker(
            failure_threshold=config.ratelimit.circuit_breaker_failure_threshold,
            open_seconds=config.ratelimit.circuit_breaker_open_seconds,
        )
        self._resolver = None  # 由 init 注入，避免循环导入
        # user_id -> _QuotaState（请求数模式批量配额）
        self._quota_states: dict = {}
        # 全局兜底滑动窗口: fallback_key -> deque[monotonic]
        self._fallback_windows: dict = defaultdict(deque)
        self._stats = {
            "redis_error_total": 0,
            "degrade_passthrough_total": 0,
            "degrade_reject_total": 0,
            "fallback_allow_total": 0,
            "fallback_reject_total": 0,
            "batch_reserve_total": 0,
            "circuit_fast_fail_total": 0,
            "last_redis_error": "",
            "last_redis_error_at": 0.0,
        }

    async def init(self, resolver=None):
        """初始化 Redis 连接、Lua 脚本与解析器"""
        self._redis = aioredis.from_url(
            config.redis_url,
            max_connections=config.redis.pool_size,
            decode_responses=True,
        )
        self._resolver = resolver
        await self._load_scripts()
        logger.info("Rate limiter initialized (batch=%d, breaker=%d/%ds)",
                    config.ratelimit.quota_batch_size,
                    config.ratelimit.circuit_breaker_failure_threshold,
                    int(config.ratelimit.circuit_breaker_open_seconds))

    async def _load_scripts(self):
        """加载/重新加载 Lua 脚本（Redis 重启后脚本缓存丢失时调用）"""
        self._request_sha = await self._redis.script_load(
            _load_script("rate_limit_request.lua")
        )
        self._token_check_sha = await self._redis.script_load(
            _load_script("rate_limit_token_check.lua")
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

    # ---------- 对外主入口 ----------

    async def check(self, api_key: str) -> RateLimitResult:
        """检查请求是否允许通过"""
        key_hash = self.hash_key(api_key)

        # 1. 三级解析（L1 → Redis → MySQL，内部已捕获异常，不会抛出）
        group, conf = await self._resolver.resolve(api_key, key_hash)

        if group is None:
            if config.ratelimit.on_key_not_found == "passthrough":
                return RateLimitResult(allowed=True, reason="key_not_found_passthrough")
            return RateLimitResult(allowed=False, reason="key_not_found")

        if conf is None:
            if config.ratelimit.on_config_not_found == "passthrough":
                return RateLimitResult(allowed=True, group=group,
                                       reason="config_not_found_passthrough")
            return RateLimitResult(allowed=False, group=group, reason="config_not_found")

        user_id = group if conf.get("scope") == "group" else key_hash
        limits = (int(conf["5h"]), int(conf["7d"]), int(conf["30d"]))

        if conf.get("type") == "token":
            return await self._check_token(user_id, group, limits)
        return await self._check_request(user_id, group, limits)

    async def deduct_tokens(self, user_id: str, tokens: int):
        """Token 模式专用：请求完成后扣减 token 用量"""
        now = int(time.time())
        for attempt in (1, 2):
            try:
                await self._call_script(
                    "_token_deduct_sha", 1, user_id, now, tokens
                )
                return
            except Exception as e:
                if _is_noscript(e) and attempt == 1:
                    try:
                        await self._load_scripts()
                        continue
                    except Exception as reload_err:
                        logger.error(f"Lua script reload failed: {reload_err}")
                logger.error(f"Token deduct failed: {e}")
                return

    async def get_group_config(self, group: str) -> Optional[dict]:
        """获取分组的限速配置（走三级解析）"""
        return await self._resolver._resolve_config(group)

    # ---------- 请求数模式：批量配额 ----------

    async def _check_request(self, user_id: str, group: str, limits) -> RateLimitResult:
        now = int(time.time())
        st = self._quota_states.get(user_id)
        if st is None:
            st = self._quota_states[user_id] = _QuotaState()
        st.last_used = time.monotonic()
        st.trim(18000, now)

        # 本地已知 5h 窗口超限（deque 覆盖整个 5h，flush 之前也生效）
        if len(st.deque) >= limits[0]:
            return RateLimitResult(allowed=False, reason="5h_exceeded", group=group,
                                   remaining={"5h": 0})

        # 本地还有预取配额：直接消耗，不访问 Redis
        if st.reserved > 0:
            st.reserved -= 1
            st.deque.append(now)
            for name in ("5h", "7d", "30d"):
                if name in st.remaining and st.remaining[name] > 0:
                    st.remaining[name] -= 1
            self._cleanup_quota_states()
            return RateLimitResult(allowed=True, group=group, remaining=dict(st.remaining))

        # 预取配额耗尽：flush 本地计数 + 检查 + 重新预取（单次往返）
        flush = list(st.deque)[-_MAX_FLUSH_BATCH:] if st.deque else []
        batch = max(1, config.ratelimit.quota_batch_size)
        try:
            result = await self._call_script(
                "_request_sha", 1, user_id,
                now, batch, limits[0], limits[1], limits[2],
                json.dumps(flush),
            )
        except Exception as e:
            return await self._degrade(user_id, e, group=group, limits=limits)

        self._stats["batch_reserve_total"] += 1
        code = result[0]
        if code == 0:
            window = result[1] if len(result) > 1 else "unknown"
            st.remaining = {window: 0}
            return RateLimitResult(allowed=False, reason=f"{window}_exceeded",
                                   group=group, remaining={window: 0})

        reserve = int(result[1]) if len(result) > 1 else 1
        st.remaining = {"5h": result[2], "7d": result[3], "30d": result[4]} \
            if len(result) > 4 else {}
        # 当前请求消耗 1 个配额
        st.reserved = max(0, reserve - 1)
        st.deque.append(now)
        for name in ("5h", "7d", "30d"):
            if name in st.remaining and st.remaining[name] > 0:
                st.remaining[name] -= 1

        self._cleanup_quota_states()
        return RateLimitResult(allowed=True, group=group, remaining=dict(st.remaining))

    def _cleanup_quota_states(self):
        if len(self._quota_states) <= _MAX_TRACKED_KEYS:
            return
        # 淘汰空闲状态（10 分钟无活动且无剩余配额）
        cutoff = time.monotonic() - 600
        idle = [k for k, v in self._quota_states.items()
                if v.last_used < cutoff and v.reserved == 0]
        for k in idle:
            del self._quota_states[k]
        if len(self._quota_states) > _MAX_TRACKED_KEYS * 2:
            self._quota_states.clear()  # 极端情况兜底，下次预取自动重建

    # ---------- Token 模式 ----------

    async def _check_token(self, user_id: str, group: str, limits) -> RateLimitResult:
        try:
            result = await self._call_script(
                "_token_check_sha", 1, user_id, limits[0], limits[1], limits[2]
            )
        except Exception as e:
            return await self._degrade(user_id, e, group=group, limits=limits)

        code = result[0]
        if code == 0:
            window = result[1] if len(result) > 1 else "unknown"
            return RateLimitResult(allowed=False, reason=f"{window}_exceeded",
                                   group=group, remaining={window: 0})
        remaining = {"5h": result[1], "7d": result[2], "30d": result[3]} \
            if len(result) > 3 else {}
        return RateLimitResult(allowed=True, group=group, remaining=remaining)

    # ---------- Redis 调用（熔断 + NOSCRIPT 自愈）----------

    async def _call_script(self, sha_attr: str, numkeys: int, *args):
        """带熔断与 NOSCRIPT 重试的 EVALSHA"""
        for attempt in (1, 2):
            if config.ratelimit.circuit_breaker_enabled and not self._breaker.allow():
                self._stats["circuit_fast_fail_total"] += 1
                raise CircuitOpenError("circuit breaker open")
            try:
                result = await self._redis.evalsha(
                    getattr(self, sha_attr), numkeys, *args
                )
                self._breaker.record_success()
                return result
            except Exception as e:
                if not _is_noscript(e):
                    self._breaker.record_failure()
                if _is_noscript(e) and attempt == 1:
                    logger.warning("NOSCRIPT detected (Redis restarted?), reloading Lua scripts")
                    try:
                        await self._load_scripts()
                        continue
                    except Exception as reload_err:
                        logger.error(f"Lua script reload failed: {reload_err}")
                raise

    # ---------- Redis 故障降级 ----------

    async def _degrade(self, user_id: str, err: Exception,
                       group: Optional[str] = None, limits=None) -> RateLimitResult:
        """Redis 异常时按配置策略降级（已解析出分组策略时做分组感知兜底）"""
        self._stats["redis_error_total"] += 1
        self._stats["last_redis_error"] = f"{type(err).__name__}: {err}"[:300]
        self._stats["last_redis_error_at"] = time.time()
        logger.error(f"Redis unavailable, degrade mode={config.ratelimit.on_redis_error}: {err}")

        mode = config.ratelimit.on_redis_error
        if mode == "reject":
            self._stats["degrade_reject_total"] += 1
            return RateLimitResult(allowed=False, reason="redis_unavailable", group=group)

        if mode == "local_fallback":
            # 分组感知兜底：按 5h 限额 ÷ worker 数 执行本地滑动窗口
            if limits:
                divisor = config.ratelimit.fallback_limit_divisor or max(
                    1, config.server.workers
                )
                window = 18000
                max_reqs = max(1, limits[0] // divisor)
            else:  # 无策略信息时退回全局兜底参数
                window = config.ratelimit.fallback_window
                max_reqs = config.ratelimit.fallback_max_requests

            allowed = await self._local_window_allow(user_id, window, max_reqs)
            if allowed:
                self._stats["fallback_allow_total"] += 1
                return RateLimitResult(allowed=True, group=group,
                                       reason="redis_error_local_fallback")
            self._stats["fallback_reject_total"] += 1
            return RateLimitResult(allowed=False, group=group,
                                   reason="local_fallback_limit")

        # passthrough（默认）
        self._stats["degrade_passthrough_total"] += 1
        return RateLimitResult(allowed=True, group=group, reason="redis_error_passthrough")

    async def _local_window_allow(self, key: str, window: float, max_reqs: int) -> bool:
        """进程内滑动窗口（事件循环内无 await，天然无并发竞争）"""
        now = time.time()
        dq = self._fallback_windows[key]
        cutoff = now - window
        while dq and dq[0] <= cutoff:
            dq.popleft()
        if len(dq) >= max_reqs:
            allowed = False
        else:
            dq.append(now)
            allowed = True

        if len(self._fallback_windows) > _MAX_TRACKED_KEYS:
            expired = [k for k, v in self._fallback_windows.items()
                       if not v or v[-1] <= cutoff]
            for k in expired:
                del self._fallback_windows[k]
        return allowed

    # ---------- 可观测性 ----------

    async def ping(self) -> bool:
        """Redis 连通性检查（供 /health；同时反哺熔断器状态）"""
        if not self._redis:
            return False
        try:
            ok = bool(await asyncio.wait_for(self._redis.ping(), timeout=2.0))
            if ok:
                self._breaker.record_success()
            else:
                self._breaker.record_failure()
            return ok
        except Exception:
            self._breaker.record_failure()
            return False

    def stats(self) -> dict:
        """降级运行统计快照"""
        return dict(self._stats)

    def diagnostics(self) -> dict:
        """熔断器与配额状态诊断信息"""
        return {
            "circuit": self._breaker.snapshot(),
            "quota_states": len(self._quota_states),
        }


rate_limiter = RateLimiter()
