"""Key->Group 同步模块 - 从 NewAPI 数据库同步到 Redis"""

import asyncio
import hashlib
import json
import logging
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import config
from app.models import RateLimitGroup

logger = logging.getLogger(__name__)


class KeyGroupSyncer:
    """从 NewAPI 数据库同步 Key->Group 映射到 Redis"""

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
        """执行一次全量 Key->Group 同步"""
        try:
            async with self._newapi_engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT `key`, `group` FROM tokens WHERE status = 1")
                )
                rows = result.fetchall()

            pipe = self._redis.pipeline(transaction=False)
            count = 0
            for row in rows:
                api_key = row[0]
                # 防御性去除 sk- 前缀，确保与网关侧 hash_key() 使用的规范化形式一致
                if api_key and api_key.startswith("sk-"):
                    api_key = api_key[3:]
                group = row[1] if row[1] else "default"
                key_hash = hashlib.sha256(api_key.encode()).hexdigest()
                pipe.set(f"keymap:{key_hash}", group, ex=config.sync.key_map_ttl)
                count += 1

            if count > 0:
                await pipe.execute()

            logger.info(f"Synced {count} key->group mappings to Redis")
        except Exception as e:
            logger.error(f"Key->Group sync failed: {e}")

    async def sync_group_configs(self, db_session_factory):
        """从 MySQL 配置库同步分组策略到 Redis"""
        try:
            async with db_session_factory() as session:
                result = await session.execute(
                    select(RateLimitGroup).where(RateLimitGroup.status == 1)
                )
                groups = result.scalars().all()

            pipe = self._redis.pipeline(transaction=False)
            count = 0
            for group in groups:
                config_json = json.dumps(group.to_config_dict())
                pipe.set(
                    f"config:{group.group_name}",
                    config_json,
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


syncer = KeyGroupSyncer()
