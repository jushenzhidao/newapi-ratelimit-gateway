"""Key->分组->策略 三级解析器

读取链路: L1 进程内缓存(LRU+TTL) → Redis → MySQL 读透(read-through)

- 正常时 L1 挡掉绝大部分 Redis 读（TTL 内同 key 只读一次）
- Redis 挂掉/未命中时直接查 MySQL 权威源，保证分组判断不失效
- 读透结果写回 Redis（best-effort，失败不影响返回）并落入 L1
- 负缓存：MySQL 也查不到的 key 短 TTL 缓存，防止恶意 key 穿透打数据库
"""

import json
import time
import logging
from collections import OrderedDict
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.config import config
from app.models import RateLimitGroup

logger = logging.getLogger(__name__)

# 负缓存哨兵：表示「确认不存在」，与「缓存未命中」(None) 区分
_NEG = object()


class _TTLCache:
    """简易 LRU + TTL 缓存（事件循环内单线程使用，无需锁）"""

    _MISS = object()

    def __init__(self, maxsize: int, ttl: float):
        self._maxsize = max(16, maxsize)
        self._ttl = ttl
        self._data: OrderedDict = OrderedDict()  # key -> (expire_at, value)
        self.hits = 0
        self.misses = 0

    def get(self, key):
        item = self._data.get(key, self._MISS)
        if item is self._MISS:
            self.misses += 1
            return None
        expire_at, value = item
        if time.monotonic() > expire_at:
            self._data.pop(key, None)
            self.misses += 1
            return None
        self._data.move_to_end(key)
        self.hits += 1
        return value

    def put(self, key, value, ttl: Optional[float] = None):
        self._data[key] = (time.monotonic() + (ttl if ttl is not None else self._ttl), value)
        self._data.move_to_end(key)
        while len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def __len__(self):
        return len(self._data)


class GroupResolver:
    """keymap / config 三级解析"""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._config_db: Optional[async_sessionmaker] = None
        self._newapi_engine: Optional[AsyncEngine] = None
        self._keymap_cache: Optional[_TTLCache] = None
        self._config_cache: Optional[_TTLCache] = None
        self._stats = {
            "l1_hit_total": 0,
            "redis_hit_total": 0,
            "db_read_total": 0,
            "negative_total": 0,
            "db_error_total": 0,
        }

    async def init(
        self,
        redis_client: aioredis.Redis,
        config_db_factory: async_sessionmaker,
        newapi_engine: AsyncEngine,
    ):
        self._redis = redis_client
        self._config_db = config_db_factory
        self._newapi_engine = newapi_engine
        self._keymap_cache = _TTLCache(
            config.ratelimit.local_cache_size, config.ratelimit.local_cache_ttl
        )
        self._config_cache = _TTLCache(
            max(1024, config.ratelimit.local_cache_size // 10),
            config.ratelimit.local_cache_ttl,
        )
        logger.info(
            "GroupResolver initialized (L1 size=%d ttl=%ds)",
            config.ratelimit.local_cache_size,
            config.ratelimit.local_cache_ttl,
        )

    async def close(self):
        # engine 均为外部传入（main.py / syncer 所有），此处不负责释放
        pass

    async def resolve(self, api_key: str, key_hash: str):
        """返回 (group, config_dict)，均可能为 None（未找到）"""
        group = await self._resolve_group(api_key, key_hash)
        if group is None:
            return None, None
        conf = await self._resolve_config(group)
        return group, conf

    # ---------- keymap: key_hash -> group ----------

    async def _resolve_group(self, api_key: str, key_hash: str) -> Optional[str]:
        v = self._keymap_cache.get(key_hash)
        if v is not None:
            self._stats["l1_hit_total"] += 1
            return None if v is _NEG else v

        # Redis
        try:
            v = await self._redis.get(f"keymap:{key_hash}")
        except Exception:
            v = None  # Redis 不可用，继续走 MySQL
        if v:
            self._stats["redis_hit_total"] += 1
            self._keymap_cache.put(key_hash, v)
            return v

        # MySQL 读透（tokens 表存明文 key）
        group = await self._db_lookup_group(api_key)
        if group:
            self._stats["db_read_total"] += 1
            self._keymap_cache.put(key_hash, group)
            await self._writeback_keymap(key_hash, group)
            return group

        # 负缓存（短 TTL，防穿透）
        self._stats["negative_total"] += 1
        self._keymap_cache.put(
            key_hash, _NEG, ttl=min(config.ratelimit.local_cache_negative_ttl,
                                    config.ratelimit.local_cache_ttl)
        )
        return None

    async def _db_lookup_group(self, api_key: str) -> Optional[str]:
        try:
            async with self._newapi_engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT `group` FROM tokens WHERE `key` = :k AND status = 1"),
                    {"k": api_key},
                )
                row = result.fetchone()
                return (row[0] or "default") if row else None
        except Exception as e:
            self._stats["db_error_total"] += 1
            logger.error(f"MySQL keymap lookup failed: {e}")
            return None

    async def _writeback_keymap(self, key_hash: str, group: str):
        try:
            await self._redis.set(
                f"keymap:{key_hash}", group, ex=config.sync.key_map_ttl
            )
        except Exception:
            pass  # best-effort

    # ---------- config: group -> 策略 ----------

    async def _resolve_config(self, group: str) -> Optional[dict]:
        v = self._config_cache.get(group)
        if v is not None:
            self._stats["l1_hit_total"] += 1
            return None if v is _NEG else v

        try:
            config_str = await self._redis.get(f"config:{group}")
        except Exception:
            config_str = None
        if config_str:
            self._stats["redis_hit_total"] += 1
            conf = json.loads(config_str)
            self._config_cache.put(group, conf)
            return conf

        conf = await self._db_lookup_config(group)
        if conf:
            self._stats["db_read_total"] += 1
            self._config_cache.put(group, conf)
            await self._writeback_config(group, conf)
            return conf

        self._stats["negative_total"] += 1
        self._config_cache.put(
            group, _NEG, ttl=min(config.ratelimit.local_cache_negative_ttl,
                                 config.ratelimit.local_cache_ttl)
        )
        return None

    async def _db_lookup_config(self, group: str) -> Optional[dict]:
        try:
            async with self._config_db() as session:
                result = await session.execute(
                    select(RateLimitGroup).where(
                        RateLimitGroup.group_name == group,
                        RateLimitGroup.status == 1,
                    )
                )
                row = result.scalars().first()
                return row.to_config_dict() if row else None
        except Exception as e:
            self._stats["db_error_total"] += 1
            logger.error(f"MySQL config lookup failed: {e}")
            return None

    async def _writeback_config(self, group: str, conf: dict):
        try:
            await self._redis.set(
                f"config:{group}", json.dumps(conf), ex=config.sync.config_cache_ttl
            )
        except Exception:
            pass

    # ---------- 统计 ----------

    def stats(self) -> dict:
        s = dict(self._stats)
        s["l1_keymap_size"] = len(self._keymap_cache) if self._keymap_cache else 0
        s["l1_config_size"] = len(self._config_cache) if self._config_cache else 0
        return s


resolver = GroupResolver()
