"""高可用重构冒烟测试：熔断器 / L1+MySQL读透 / 批量配额 / 分组感知兜底"""
import asyncio
import json

from app.config import config
from app.ratelimit import RateLimiter
from app.resolver import GroupResolver
from redis.exceptions import ResponseError, ConnectionError as RC

config.ratelimit.quota_batch_size = 3
config.server.workers = 16
config.ratelimit.circuit_breaker_failure_threshold = 5
config.ratelimit.circuit_breaker_open_seconds = 10.0

WINDOW_NAMES = ("5h", "7d", "30d")


class FakeRedis:
    """模拟 Redis，含 Lua 脚本语义"""

    def __init__(self):
        self.store = {}
        self.down = False
        self.scripts_ok = set()
        self.calls = {"evalsha": 0, "get": 0, "set": 0}
        self.last_args = None

    async def script_load(self, s):
        return "sha_" + s[:10].replace("\n", "").replace(" ", "")

    async def evalsha(self, sha, *args):
        self.calls["evalsha"] += 1
        self.last_args = args
        if self.down:
            raise RC("down")
        if sha not in self.scripts_ok:
            raise ResponseError("NOSCRIPT No matching script")
        uid = args[1]  # args[0] = numkeys
        if len(args) == 5:  # token_check: numkeys, uid, l5h, l7d, l30d
            limits = args[2:]
            return [1, limits[0], limits[1], limits[2]]
        # request: numkeys, uid, now, batch, l5h, l7d, l30d, flush_json
        _, _, now, batch, l5h, l7d, l30d, flush_json = args
        flush = json.loads(flush_json)
        windows = [(18000, l5h), (604800, l7d), (2592000, l30d)]
        avail, rem = None, []
        for i, (ttl, limit) in enumerate(windows):
            k = f"rl:{uid}:{ttl}"
            arr = [t for t in self.store.get(k, []) if t > now - ttl] + flush
            self.store[k] = arr
            count = len(arr)
            rem.append(limit - count)
            if count >= limit:
                return [0, WINDOW_NAMES[i], limit, count]
            a = limit - count
            if avail is None or a < avail:
                avail = a
        return [1, min(batch, avail), rem[0], rem[1], rem[2]]

    async def get(self, k):
        self.calls["get"] += 1
        if self.down:
            raise RC("down")
        return self.store.get(k)

    async def set(self, k, v, ex=None):
        self.calls["set"] += 1
        if self.down:
            raise RC("down")
        self.store[k] = v
        return True


class FakeRow:
    def __init__(self, conf):
        self._conf = conf

    def to_config_dict(self):
        return self._conf


class FakeScalars:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class FakeExecResult:
    def __init__(self, row):
        self._row = row

    def scalars(self):
        return FakeScalars(self._row)


class FakeConfigSession:
    def __init__(self, groups):
        self._groups = groups

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute(self, q):
        # 从 select 的编译参数里取 group 名（仅测试用）
        params = q.compile().params if hasattr(q, "compile") else {}
        name = None
        for v in params.values():
            if isinstance(v, str):
                name = v
                break
        if name and name in self._groups:
            return FakeExecResult(FakeRow(self._groups[name]))
        return FakeExecResult(None)


def make_config_factory(groups):
    def factory():
        return FakeConfigSession(groups)
    return factory


class FakeNewapiConn:
    def __init__(self, engine):
        self._engine = engine

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute(self, sql, params=None):
        self._engine.queries += 1
        group = self._engine.tokens.get(params["k"]) if params else None
        return FakeFetch(group)


class FakeFetch:
    def __init__(self, group):
        self._group = group

    def fetchone(self):
        return (self._group,) if self._group else None


class FakeNewapiEngine:
    def __init__(self, tokens):
        self.tokens = tokens
        self.queries = 0

    def connect(self):
        return FakeNewapiConn(self)


GROUPS = {
    "default": {"5h": 160, "7d": 1000, "30d": 5000, "type": "request", "scope": "key"},
    "tok": {"5h": 100000, "7d": 999999, "30d": 9999999, "type": "token", "scope": "key"},
}


async def main():
    rl = RateLimiter()
    rsv = GroupResolver()
    fake = FakeRedis()
    rl._redis = fake
    rl._request_sha, rl._token_check_sha, rl._token_deduct_sha = "s_req", "s_tok", "s_ded"
    fake.scripts_ok.update({"s_req", "s_tok", "s_ded"})
    engine = FakeNewapiEngine({"sk-1": "default", "sk-tok": "tok", "sk-db1": "default",
                               "sk-brk": "default"})
    await rsv.init(fake, make_config_factory(GROUPS), engine)
    rl._resolver = rsv

    # ---- T1 批量配额：3 次请求只打 1 次 Redis ----
    results = [await rl.check("sk-1") for _ in range(3)]
    assert all(r.allowed for r in results), [r.reason for r in results]
    assert fake.calls["evalsha"] == 1, fake.calls
    r4 = await rl.check("sk-1")
    assert r4.allowed and fake.calls["evalsha"] == 2
    # 第 4 次触发新预取，flush 应带上前 3 次的本地计数
    flush = json.loads(fake.last_args[-1])
    assert len(flush) == 3, flush
    print("T1 批量配额: OK (3 请求 1 次 EVALSHA, flush=3)")

    # ---- T2 L1 缓存 ----
    gets_before = fake.calls["get"]
    await rl.check("sk-1")
    assert fake.calls["get"] == gets_before, "L1 未命中"
    print("T2 L1 缓存: OK (TTL 内不再读 Redis)")

    # ---- T3 MySQL 读透 + 写回 + 负缓存 ----
    q_before = engine.queries
    r = await rl.check("sk-db1")
    assert r.group == "default" and r.allowed
    assert engine.queries == q_before + 1  # keymap 查一次（config 走 config 库）
    assert fake.store.get("keymap:" + rl.hash_key("sk-db1")) == "default"  # 写回
    r = await rl.check("sk-db1")
    assert engine.queries == q_before + 1  # L1 生效不再查库
    r = await rl.check("sk-unknown")
    assert r.allowed and r.reason == "key_not_found_passthrough"
    neg_q = engine.queries
    r = await rl.check("sk-unknown")
    assert engine.queries == neg_q  # 负缓存生效
    print("T3 MySQL 读透/写回/负缓存: OK")

    # ---- T4 熔断器：连续失败→打开→快速失败→半开恢复 ----
    config.ratelimit.on_redis_error = "reject"
    fake.down = True
    for _ in range(5):
        r = await rl.check("sk-brk")
        assert not r.allowed and r.reason == "redis_unavailable"
    es_after_fail = fake.calls["evalsha"]
    assert rl._breaker.state == "open", rl._breaker.state
    r = await rl.check("sk-brk")  # 熔断打开，应快速失败不碰 Redis
    assert not r.allowed
    assert fake.calls["evalsha"] == es_after_fail, "熔断后不应再调用 Redis"
    assert rl.stats()["circuit_fast_fail_total"] >= 1
    # 半开恢复
    rl._breaker._opened_at -= 11  # 模拟 open_seconds 已过
    fake.down = False
    r = await rl.check("sk-brk")
    assert r.allowed, r.reason
    assert rl._breaker.state == "closed"
    print("T4 熔断器: OK (open→fast-fail→half-open→closed)")

    # ---- T5 分组感知兜底：5h=160, workers=16 → 本地限 10 ----
    config.ratelimit.on_redis_error = "local_fallback"
    fake.down = True
    rl._quota_states.clear()
    # 熔断器直接打开，跳过 Redis
    allowed = [(await rl.check("sk-1")).allowed for _ in range(11)]
    assert allowed == [True] * 10 + [False], allowed
    last = await rl.check("sk-1")
    assert last.reason == "local_fallback_limit"
    print("T5 分组感知兜底: OK (160÷16=10, 第 11 次拒绝)")

    # ---- T6 token 模式 ----
    fake.down = False
    rl._breaker._state = "closed"  # 复位熔断器（T5 残留 open 状态）
    rl._breaker._failures = 0
    fake.scripts_ok.add("s_tok")
    r = await rl.check("sk-tok")
    assert r.allowed and r.remaining.get("5h") == 100000, (r.allowed, r.remaining)
    print("T6 token 模式: OK", r.remaining)

    # ---- 汇总 ----
    print("\n统计:", {k: v for k, v in rl.stats().items() if not k.startswith("last")})
    print("诊断:", rl.diagnostics())
    print("解析器:", rsv.stats())
    print("\n全部 6 组断言通过")


asyncio.run(main())
