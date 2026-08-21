"""FastAPI 应用入口"""

import asyncio
import hashlib
import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from sqlalchemy import text, select, func
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.config import config
from app.ratelimit import rate_limiter
from app.resolver import resolver
from app.sync import syncer
from app.proxy import handle_proxy
from app.admin import router as admin_router
from app.models import Base, RateLimitGroup

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

# 默认种子数据（与 sql/init.sql 一致）
DEFAULT_GROUPS = [
    {"group_name": "default",    "limit_5h": 100,  "limit_7d": 1000,  "limit_30d": 5000,   "limit_type": "request", "scope": "key", "remark": "默认分组"},
    {"group_name": "vip",        "limit_5h": 500,  "limit_7d": 5000,  "limit_30d": 50000,  "limit_type": "request", "scope": "key", "remark": "VIP分组"},
    {"group_name": "enterprise", "limit_5h": 2000, "limit_7d": 20000, "limit_30d": 200000, "limit_type": "request", "scope": "key", "remark": "企业分组"},
    {"group_name": "trial",      "limit_5h": 10,   "limit_7d": 50,    "limit_30d": 200,    "limit_type": "request", "scope": "key", "remark": "试用分组"},
]


async def init_database():
    """自动建库 + 建表 + 种子数据（如果不存在）"""
    mysql_cfg = config.mysql

    # Step 1: 连接到 MySQL 服务器（不指定库名），创建数据库
    server_url = (
        f"mysql+aiomysql://{mysql_cfg.user}:{mysql_cfg.password}"
        f"@{mysql_cfg.host}:{mysql_cfg.port}/"
    )
    server_engine = create_async_engine(server_url, pool_pre_ping=True)
    try:
        async with server_engine.connect() as conn:
            await conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS `{mysql_cfg.database}` "
                f"DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci"
            ))
            await conn.commit()
        logger.info(f"Database '{mysql_cfg.database}' ready (created if not exists)")
    finally:
        await server_engine.dispose()

    # Step 2: 用主 engine 建表（IF NOT EXISTS 语义，已有表不报错）
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables verified (created if not exists)")

    # Step 3: 如果表为空，插入种子数据
    async with db_session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(RateLimitGroup))
        if count == 0:
            for g in DEFAULT_GROUPS:
                session.add(RateLimitGroup(**g))
            await session.commit()
            logger.info(f"Seeded {len(DEFAULT_GROUPS)} default group configs")
        else:
            logger.info(f"Table already has {count} rows, skipping seed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动：自动建库 + 建表 + 种子数据
    if config.mysql.auto_init:
        await init_database()
    else:
        logger.info("auto_init disabled, assuming database is ready")

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
    """查询指定 API Key 的当前限速状态

    走三级解析器（L1 → Redis → MySQL），不直接读 Redis keymap/config，
    确保 Redis 刚重启或 L1 命中时也能正确返回分组和配置。
    配额计数合并 Redis 已 flush + 本地未 flush，比纯读 Redis 更准确。
    """
    api_key = rate_limiter.normalize_key(api_key)
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    try:
        # 三级解析：L1 缓存 → Redis → MySQL 读透
        group, conf = await resolver.resolve(api_key, key_hash)
        if not group:
            return {"found": False, "message": "Key not found (not in any group)"}
        if not conf:
            return {"found": True, "group": group, "message": "Group config not found"}

        user_id = group if conf.get("scope") == "group" else key_hash
        status = await rate_limiter.get_quota_status(user_id, conf)

        return {"found": True, "group": group, "config": conf, "status": status}
    except Exception as e:
        return {"found": False, "error": f"lookup_failed: {e}"}


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def catch_all(request: Request):
    """捕获所有非管理/健康检查路径，代理到 NewAPI"""
    return await handle_proxy(request)
