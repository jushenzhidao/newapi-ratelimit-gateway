"""管理后台 API - 分组限速策略 CRUD"""

import json
import logging
from typing import List

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import config
from app.models import RateLimitGroup
from app.schemas import GroupCreate, GroupUpdate, GroupResponse
from app.ratelimit import rate_limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


async def verify_auth(authorization: str = Header(None)):
    if not config.admin.enabled:
        raise HTTPException(status_code=403, detail="Admin API disabled")
    token = ""
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
    if token != config.admin.auth_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")


async def _sync_group_to_redis(group: RateLimitGroup):
    if group.status == 1:
        config_json = json.dumps(group.to_config_dict())
        await rate_limiter._redis.set(
            f"config:{group.group_name}",
            config_json,
            ex=config.sync.config_cache_ttl,
        )
    else:
        await rate_limiter._redis.delete(f"config:{group.group_name}")


def _to_response(group: RateLimitGroup) -> GroupResponse:
    return GroupResponse(
        id=group.id,
        group_name=group.group_name,
        limit_5h=group.limit_5h,
        limit_7d=group.limit_7d,
        limit_30d=group.limit_30d,
        limit_type=group.limit_type,
        scope=group.scope,
        status=group.status,
        remark=group.remark,
        created_at=group.created_at.strftime("%Y-%m-%d %H:%M:%S") if group.created_at else "",
        updated_at=group.updated_at.strftime("%Y-%m-%d %H:%M:%S") if group.updated_at else "",
    )


@router.post("/groups", response_model=GroupResponse, dependencies=[Depends(verify_auth)])
async def create_group(data: GroupCreate):
    from app.main import db_session_factory
    async with db_session_factory() as session:
        group = RateLimitGroup(
            group_name=data.group_name,
            limit_5h=data.limit_5h,
            limit_7d=data.limit_7d,
            limit_30d=data.limit_30d,
            limit_type=data.limit_type,
            scope=data.scope,
            remark=data.remark,
        )
        session.add(group)
        try:
            await session.commit()
        except Exception as e:
            await session.rollback()
            raise HTTPException(status_code=400, detail=f"Create failed: {e}")
        await session.refresh(group)
        await _sync_group_to_redis(group)
        return _to_response(group)


@router.get("/groups", response_model=List[GroupResponse], dependencies=[Depends(verify_auth)])
async def list_groups():
    from app.main import db_session_factory
    async with db_session_factory() as session:
        result = await session.execute(select(RateLimitGroup).order_by(RateLimitGroup.id))
        groups = result.scalars().all()
        return [_to_response(g) for g in groups]


@router.get("/groups/{group_name}", response_model=GroupResponse, dependencies=[Depends(verify_auth)])
async def get_group(group_name: str):
    from app.main import db_session_factory
    async with db_session_factory() as session:
        result = await session.execute(
            select(RateLimitGroup).where(RateLimitGroup.group_name == group_name)
        )
        group = result.scalar_one_or_none()
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        return _to_response(group)


@router.put("/groups/{group_name}", response_model=GroupResponse, dependencies=[Depends(verify_auth)])
async def update_group(group_name: str, data: GroupUpdate):
    from app.main import db_session_factory
    async with db_session_factory() as session:
        result = await session.execute(
            select(RateLimitGroup).where(RateLimitGroup.group_name == group_name)
        )
        group = result.scalar_one_or_none()
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(group, key, value)

        await session.commit()
        await session.refresh(group)
        await _sync_group_to_redis(group)
        return _to_response(group)


@router.delete("/groups/{group_name}", dependencies=[Depends(verify_auth)])
async def delete_group(group_name: str):
    from app.main import db_session_factory
    async with db_session_factory() as session:
        result = await session.execute(
            select(RateLimitGroup).where(RateLimitGroup.group_name == group_name)
        )
        group = result.scalar_one_or_none()
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        group.status = 0
        await session.commit()
        await rate_limiter._redis.delete(f"config:{group_name}")
        return {"message": f"Group '{group_name}' disabled"}


@router.post("/sync", dependencies=[Depends(verify_auth)])
async def manual_sync():
    from app.sync import syncer
    await syncer.sync_once()
    return {"message": "Sync triggered"}
