"""Pydantic 请求/响应模型"""

from pydantic import BaseModel, Field
from typing import Optional, Literal


class GroupCreate(BaseModel):
    group_name: str = Field(..., max_length=64, description="分组名称")
    limit_5h: int = Field(..., ge=1, description="5小时配额")
    limit_7d: int = Field(..., ge=1, description="7天配额")
    limit_30d: int = Field(..., ge=1, description="30天配额")
    limit_type: Literal["request", "token"] = Field(default="request", description="限速类型")
    scope: Literal["key", "group"] = Field(default="key", description="限速粒度")
    remark: Optional[str] = Field(None, max_length=256, description="备注")


class GroupUpdate(BaseModel):
    limit_5h: Optional[int] = Field(None, ge=1)
    limit_7d: Optional[int] = Field(None, ge=1)
    limit_30d: Optional[int] = Field(None, ge=1)
    limit_type: Optional[Literal["request", "token"]] = None
    scope: Optional[Literal["key", "group"]] = None
    status: Optional[int] = Field(None, ge=0, le=1)
    remark: Optional[str] = Field(None, max_length=256)


class GroupResponse(BaseModel):
    id: int
    group_name: str
    limit_5h: int
    limit_7d: int
    limit_30d: int
    limit_type: str
    scope: str
    status: int
    remark: Optional[str]
    created_at: str
    updated_at: str


class ErrorResponse(BaseModel):
    error: str
    message: str
    remaining: Optional[dict] = None
