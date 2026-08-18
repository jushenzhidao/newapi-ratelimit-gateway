"""SQLAlchemy 数据模型"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Enum, Text, DateTime, Index
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class RateLimitGroup(Base):
    """分组限速策略配置表"""
    __tablename__ = "rate_limit_group"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_name = Column(String(64), nullable=False, unique=True, comment="分组名称")
    limit_5h = Column(Integer, nullable=False, default=100, comment="5小时配额")
    limit_7d = Column(Integer, nullable=False, default=1000, comment="7天配额")
    limit_30d = Column(Integer, nullable=False, default=5000, comment="30天配额")
    limit_type = Column(Enum("request", "token"), nullable=False, default="request", comment="限速类型")
    scope = Column(Enum("key", "group"), nullable=False, default="key", comment="限速粒度")
    status = Column(Integer, nullable=False, default=1, comment="1=启用 0=停用")
    remark = Column(String(256), nullable=True, comment="备注")
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    def to_config_dict(self) -> dict:
        """转为 Redis 缓存的 JSON 格式"""
        return {
            "5h": self.limit_5h,
            "7d": self.limit_7d,
            "30d": self.limit_30d,
            "type": self.limit_type,
            "scope": self.scope,
        }
