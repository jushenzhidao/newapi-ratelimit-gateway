"""FastAPI 应用入口"""

import asyncio
import hashlib
import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.config import config
from app.ratelimit import rate_limiter
from app.resolver import resolver
from app.sync import syncer
from app.proxy import handle_proxy
from app.admin import router as admin_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

db_engine = create_async_engine(
    config.mysql_url,
    pool_size=config.mysql.pool_size,
    pool_pre_ping=True,
)
db_session_factory = async_sessionmaker(db_engine, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动
    await rate_limiter.init(resolver=resolver)
    await syncer.init(rate_limiter._redis)
    await resolver.init(rate_limiter._redis, db_session_factory, syncer._newapi_engine)
    await syncer.sync_once()
    await syncer.sync_group_configs(db_session_factory)
    sync_task = asyncio.create_task(syncer.run_loop(db_session_factory))
    logger.info(f"Gateway started, proxying to {config.newapi.base_url}")
    yield
    # 关闭
    sync_task.cancel()
    try:
        await sync_task
    except asyncio.CancelledError:
        pass
    await resolver.close()
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

app.include_router(admin_router)


@app.get("/health")
async def health():
    """健康检查：网关存活 + Redis 连通性 + 降级统计

    注意：Redis down 时仍返回 200（status=degraded），避免被负载均衡摘除
    导致代理能力整体丢失；告警系统应按 status 字段触发。
    """
    redis_ok = await rate_limiter.ping()
    body = {
        "status": "ok" if redis_ok else "degraded",
        "redis": "up" if redis_ok else "down",
        "degrade_mode": config.ratelimit.on_redis_error,
    }
    body.update(rate_limiter.stats())
    body.update(rate_limiter.diagnostics())
    body.update({f"resolver_{k}": v for k, v in resolver.stats().items()})
    return body


@app.get("/ratelimit/status/{api_key}")
async def ratelimit_status(api_key: str):
    """查询指定 API Key 的当前限速状态"""
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    try:
        group = await rate_limiter._redis.get(f"keymap:{key_hash}")
        if not group:
            return {"found": False, "message": "Key not found in cache"}

        config_str = await rate_limiter._redis.get(f"config:{group}")
        if not config_str:
            return {"found": False, "group": group, "message": "Config not found in cache"}

        conf = json.loads(config_str)
        now = int(time.time())
        windows = [("5h", 18000, conf["5h"]), ("7d", 604800, conf["7d"]), ("30d", 2592000, conf["30d"])]

        user_id = group if conf.get("scope") == "group" else key_hash
        status = {}
        for name, ttl, limit in windows:
            if conf.get("type") == "token":
                used = int(await rate_limiter._redis.get(f"token_usage:{user_id}:{ttl}") or 0)
            else:
                key = f"ratelimit:{user_id}:{ttl}"
                await rate_limiter._redis.zremrangebyscore(key, 0, now - ttl)
                used = await rate_limiter._redis.zcard(key)
            status[name] = {"used": used, "limit": limit, "remaining": max(0, limit - used)}

        return {"found": True, "group": group, "config": conf, "status": status}
    except Exception as e:
        return {"found": False, "error": f"redis_unavailable: {e}"}


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def catch_all(request: Request):
    """捕获所有非管理/健康检查路径，代理到 NewAPI"""
    return await handle_proxy(request)
