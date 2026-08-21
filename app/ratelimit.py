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
        user_id: Optional[str] = None,
        mode: Optional[str] = None,
        limits: Optional[tuple] = None,
    ):
        self.allowed = allowed
        self.reason = reason
        self.group = group
        self.remaining = remaining or {}
        self.user_id = user_id   # 限速主体（key hash 或 group 名）
        self.mode = mode         # "request" / "token" / None（passthrough 无消耗）
        self.limits = limits     # (5h, 7d, 30d)


class _QuotaState:
    """请求数模式的本地配额状态（每 worker 独立）"""

    __slots__ = ("deque", "reserved", "remaining", "_redis_remaining", "_last_flush_at", "last_used")

    def __init__(self):
        self.deque = deque()      # 本地已消耗但未 flush 的请求时间戳（flush 后清空）
        self.reserved = 0         # 本地剩余预取配额
        self.remaining = {}       # 对外展示的各窗口剩余（随本地消耗递减）
        self._redis_remaining = {}  # flush 时 Redis 侧的剩余快照（不随本地消耗变化，用于本地超限检查）
        self._last_flush_at = 0.0    # 上次 Lua flush 的 monotonic 时间（用于判断快照是否过期）
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
            "refund_total": 0,
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
    def normalize_key(api_key: str) -> str:
        """去除 sk- 前缀，与 NewAPI 数据库 tokens.key 列的存储格式保持一致。

        NewAPI 在查询 tokens 表前会 strip 掉 sk- 前缀（见 NewAPI 源码
        model/token.go: strings.TrimPrefix(token, "sk-")），因此数据库中
        存储的是不带前缀的原始 key。本方法确保网关侧的哈希和查询使用
        相同的规范化形式，避免 keymap 查不到导致限速静默失效。
        """
        if api_key and api_key.startswith("sk-"):
            return api_key[3:]
        return api_key

    @staticmethod
    def hash_key(api_key: str) -> str:
        """对 API Key 做 SHA256 哈希（自动去除 sk- 前缀）"""
        api_key = RateLimiter.normalize_key(api_key)
        return hashlib.sha256(api_key.encode()).hexdigest()

    # ---------- 对外主入口 ----------

    async def check(self, api_key: str) -> RateLimitResult:
        """检查请求是否允许通过"""
        api_key = self.normalize_key(api_key)
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

    async def refund(self, rl_result: RateLimitResult) -> None:
        """退还一次请求配额（上游请求失败时调用，无论失败原因）

        仅 request 模式有效（token 模式失败本就不扣减，refund 为 no-op）。

        退还策略：
        1. Redis 故障期间的本地兜底窗口 → 弹出兜底 deque 最新一条
        2. 本地未 flush 的计数还在 deque → 弹出最新一条 + reserved 回增
           （滑动窗口按计数语义生效，弹出的时间戳属于哪次请求不影响总量正确性）
        3. 该请求已 flush 到 Redis → 移除各窗口最新一条成员（ZREMRANGEBYRANK）

        精度说明：并发场景下（退还时 deque 中已有其他在途请求的时间戳），
        退还总量依然正确，最多导致极小的瞬时超发（≤ 在途失败数）。
        """
        if not rl_result or not rl_result.allowed:
            return
        if rl_result.mode != "request" or not rl_result.user_id:
            return
        # 仅正常消耗（reason=""）与本地兜底窗口消耗需要退还；
        # passthrough 等路径未产生任何计数，退还反而会误删别人的计数
        if rl_result.reason not in ("", "redis_error_local_fallback"):
            return

        self._stats["refund_total"] += 1

        # Redis 故障期间的本地兜底窗口
        if rl_result.reason == "redis_error_local_fallback":
            dq = self._fallback_windows.get(rl_result.user_id)
            if dq:
                dq.pop()
            return

        st = self._quota_states.get(rl_result.user_id)
        if st is not None and st.deque:
            # 退还本地未 flush 的一次计数：弹出最新时间戳 + 回增预取配额
            st.deque.pop()
            st.reserved += 1
            limit_map = dict(zip(("5h", "7d", "30d"), rl_result.limits)) \
                if rl_result.limits else {}
            for name in ("5h", "7d", "30d"):
                if name in st.remaining:
                    v = st.remaining[name] + 1
                    cap = limit_map.get(name)
                    st.remaining[name] = min(v, cap) if cap else v
            return

        # 该请求的时间戳已 flush 到 Redis：移除各窗口最新一条成员
        try:
            for ttl in (18000, 604800, 2592000):
                key = f"ratelimit:{rl_result.user_id}:{ttl}"
                await self._redis.zremrangebyrank(key, -1, -1)
        except Exception as e:
            # Redis 故障时放弃本次退还（误差 ≤1 次，随窗口过期自愈）
            logger.warning(f"Refund via redis failed: {e}")

    async def get_group_config(self, group: str) -> Optional[dict]:
        """获取分组的限速配置（走三级解析）"""
        return await self._resolver._resolve_config(group)

    async def get_quota_status(self, user_id: str, conf: dict) -> dict:
        """查询配额状态：Redis 已 flush 计数 + 本地未 flush 计数

        多 worker 部署时，本方法只能看到当前 worker 的本地 deque，
        其他 worker 的未 flush 计数不可见，total_used 为下界估计。
        """
        now = int(time.time())
        windows = [("5h", 18000, int(conf["5h"])),
                   ("7d", 604800, int(conf["7d"])),
                   ("30d", 2592000, int(conf["30d"]))]

        # 本地未 flush 的请求数（flush 后会写入所有窗口，因此加到所有窗口）
        st = self._quota_states.get(user_id)
        local_unflushed = len(st.deque) if st else 0

        status = {}
        for name, ttl, limit in windows:
            if conf.get("type") == "token":
                used = int(await self._redis.get(f"token_usage:{user_id}:{ttl}") or 0)
            else:
                key = f"ratelimit:{user_id}:{ttl}"
                await self._redis.zremrangebyscore(key, 0, now - ttl)
                redis_used = await self._redis.zcard(key)
                # 本地未 flush 的请求会写入所有窗口，因此全部加上
                used = redis_used + local_unflushed
            status[name] = {
                "used": used,
                "limit": limit,
                "remaining": max(0, limit - used),
            }

        return status

    # ---------- 请求数模式：批量配额 ----------

    async def _check_request(self, user_id: str, group: str, limits) -> RateLimitResult:
        now = int(time.time())
        st = self._quota_states.get(user_id)
        if st is None:
            st = self._quota_states[user_id] = _QuotaState()
        st.last_used = time.monotonic()
        st.trim(18000, now)

        # 本地已知 5h 窗口超限：本地未 flush 的请求数 >= flush 时 Redis 侧剩余
        # （总已用 = Redis 已 flush + 本地未 flush，当本地未 flush >= Redis 剩余时即超限）
        # 仅当快照新鲜（< local_cache_ttl）时才本地拦截，防止 Redis 数据已过期但
        # _redis_remaining 仍为 0 导致永久死锁（用户被 block 无法触发 Lua 刷新）
        flush_rem_5h = st._redis_remaining.get("5h", limits[0])
        snapshot_fresh = (
            st._last_flush_at > 0
            and (time.monotonic() - st._last_flush_at) < config.ratelimit.local_cache_ttl
        )
        if snapshot_fresh and flush_rem_5h > 0 and len(st.deque) >= flush_rem_5h:
            return RateLimitResult(allowed=False, reason="5h_exceeded", group=group,
                                   remaining={"5h": 0}, user_id=user_id,
                                   mode="request", limits=limits)

        # 本地还有预取配额：直接消耗，不访问 Redis
        if st.reserved > 0:
            st.reserved -= 1
            st.deque.append(now)
            for name in ("5h", "7d", "30d"):
                if name in st.remaining and st.remaining[name] > 0:
                    st.remaining[name] -= 1
            self._cleanup_quota_states()
            return RateLimitResult(allowed=True, group=group, remaining=dict(st.remaining),
                                   user_id=user_id, mode="request", limits=limits)

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
            return await self._degrade(user_id, e, group=group, limits=limits,
                                       mode="request")

        self._stats["batch_reserve_total"] += 1
        code = result[0]
        if code == 0:
            window = result[1] if len(result) > 1 else "unknown"
            st.remaining = {window: 0}
            st._redis_remaining = {window: 0}
            st._last_flush_at = time.monotonic()
            st.deque.clear()
            return RateLimitResult(allowed=False, reason=f"{window}_exceeded",
                                   group=group, remaining={window: 0},
                                   user_id=user_id, mode="request", limits=limits)

        reserve = int(result[1]) if len(result) > 1 else 1
        st.remaining = {"5h": result[2], "7d": result[3], "30d": result[4]} \
            if len(result) > 4 else {}
        # 快照 Redis 侧剩余（不随本地消耗递减，用于本地超限检查）
        st._redis_remaining = dict(st.remaining)
        st._last_flush_at = time.monotonic()
        # 清空已 flush 的 deque，防止下次 flush 重复 ZADD 导致 Redis 计数虚高
        st.deque.clear()
        # 当前请求消耗 1 个配额
        st.reserved = max(0, reserve - 1)
        st.deque.append(now)
        for name in ("5h", "7d", "30d"):
            if name in st.remaining and st.remaining[name] > 0:
                st.remaining[name] -= 1

        self._cleanup_quota_states()
        return RateLimitResult(allowed=True, group=group, remaining=dict(st.remaining),
                               user_id=user_id, mode="request", limits=limits)

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
            return await self._degrade(user_id, e, group=group, limits=limits,
                                       mode="token")

        code = result[0]
        if code == 0:
            window = result[1] if len(result) > 1 else "unknown"
            return RateLimitResult(allowed=False, reason=f"{window}_exceeded",
                                   group=group, remaining={window: 0},
                                   user_id=user_id, mode="token", limits=limits)
        remaining = {"5h": result[1], "7d": result[2], "30d": result[3]} \
            if len(result) > 3 else {}
        return RateLimitResult(allowed=True, group=group, remaining=remaining,
                               user_id=user_id, mode="token", limits=limits)

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
                       group: Optional[str] = None, limits=None,
                       mode: str = "request") -> RateLimitResult:
        """Redis 异常时按配置策略降级（已解析出分组策略时做分组感知兜底）"""
        self._stats["redis_error_total"] += 1
        self._stats["last_redis_error"] = f"{type(err).__name__}: {err}"[:300]
        self._stats["last_redis_error_at"] = time.time()
        logger.error(f"Redis unavailable, degrade mode={config.ratelimit.on_redis_error}: {err}")

        mode_cfg = config.ratelimit.on_redis_error
        if mode_cfg == "reject":
            self._stats["degrade_reject_total"] += 1
            return RateLimitResult(allowed=False, reason="redis_unavailable", group=group,
                                   user_id=user_id, mode=mode, limits=limits)

        if mode_cfg == "local_fallback":
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
                                       reason="redis_error_local_fallback",
                                       user_id=user_id, mode=mode, limits=limits)
            self._stats["fallback_reject_total"] += 1
            return RateLimitResult(allowed=False, group=group,
                                   reason="local_fallback_limit",
                                   user_id=user_id, mode=mode, limits=limits)

        # passthrough（默认）
        self._stats["degrade_passthrough_total"] += 1
        return RateLimitResult(allowed=True, group=group, reason="redis_error_passthrough",
                               user_id=user_id, mode=mode, limits=limits)

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
