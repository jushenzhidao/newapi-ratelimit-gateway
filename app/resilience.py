"""熔断器 - Redis 连续故障时快速失败，避免每请求等待超时导致延迟雪崩

状态机:
- closed    : 正常，放行所有 Redis 调用；连续失败达到阈值后跳到 open
- open      : 熔断，所有 Redis 调用立即失败（不发起网络请求），等待恢复窗口
- half-open : 恢复窗口到期后放行一个探测请求；成功 → closed，失败 → open

单进程内使用（asyncio 单线程事件循环，无需锁）。
"""

import time


class CircuitOpenError(Exception):
    """熔断器打开，调用方应立即走降级路径"""


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, open_seconds: float = 10.0):
        self._failure_threshold = max(1, failure_threshold)
        self._open_seconds = max(0.5, open_seconds)
        self._state = "closed"
        self._failures = 0
        self._opened_at = 0.0
        self._probe_inflight = False

    @property
    def state(self) -> str:
        if self._state == "open" and (time.monotonic() - self._opened_at) >= self._open_seconds:
            return "half-open"
        return self._state

    def allow(self) -> bool:
        """是否放行本次 Redis 调用"""
        state = self.state
        if state == "closed":
            return True
        if state == "open":
            return False
        # half-open: 只放行一个探测请求，其余快速失败
        if self._probe_inflight:
            return False
        self._probe_inflight = True
        return True

    def record_success(self):
        if self._state != "closed":
            self._state = "closed"
        self._failures = 0
        self._probe_inflight = False

    def record_failure(self):
        self._probe_inflight = False
        if self._state == "half-open" or self.state == "half-open":
            # 探测失败，立即重新熔断
            self._trip()
            return
        self._failures += 1
        if self._failures >= self._failure_threshold:
            self._trip()

    def _trip(self):
        self._state = "open"
        self._opened_at = time.monotonic()
        self._failures = 0

    def snapshot(self) -> dict:
        return {"state": self.state, "failures": self._failures}
