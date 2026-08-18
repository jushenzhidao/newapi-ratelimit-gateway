"""配置管理 - 从 config.yaml 加载"""

from pathlib import Path
from pydantic_settings import BaseSettings
import yaml
from typing import Optional


class ServerConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    workers: int = 4


class NewAPIConfig(BaseSettings):
    base_url: str = "http://127.0.0.1:3000"
    timeout: int = 300


class RedisConfig(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 6379
    password: str = ""
    db: int = 0
    pool_size: int = 50


class MySQLConfig(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "ratelimit_gateway"
    pool_size: int = 10


class NewAPIMySQLConfig(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "readonly_user"
    password: str = ""
    database: str = "new-api"
    pool_size: int = 5


class SyncConfig(BaseSettings):
    key_group_interval: int = 60
    config_cache_ttl: int = 60
    key_map_ttl: int = 300


class RateLimitConfig(BaseSettings):
    on_key_not_found: str = "passthrough"
    on_config_not_found: str = "passthrough"
    show_remaining: bool = True
    # token 模式 + 流式请求时，自动注入 stream_options.include_usage=true
    # 设为 false 如果上游 LLM 不支持此参数
    auto_inject_include_usage: bool = True


class AdminConfig(BaseSettings):
    enabled: bool = True
    auth_token: str = "change-me-in-production"


class AppConfig:
    """全局配置单例"""

    def __init__(self):
        config_path = Path(__file__).parent.parent / "config.yaml"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        else:
            data = {}

        self.server = ServerConfig(**data.get("server", {}))
        self.newapi = NewAPIConfig(**data.get("newapi", {}))
        self.redis = RedisConfig(**data.get("redis", {}))
        self.mysql = MySQLConfig(**data.get("mysql", {}))
        self.newapi_mysql = NewAPIMySQLConfig(**data.get("newapi_mysql", {}))
        self.sync = SyncConfig(**data.get("sync", {}))
        self.ratelimit = RateLimitConfig(**data.get("ratelimit", {}))
        self.admin = AdminConfig(**data.get("admin", {}))

    @property
    def redis_url(self) -> str:
        auth = f":{self.redis.password}@" if self.redis.password else ""
        return f"redis://{auth}{self.redis.host}:{self.redis.port}/{self.redis.db}"

    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+aiomysql://{self.mysql.user}:{self.mysql.password}"
            f"@{self.mysql.host}:{self.mysql.port}/{self.mysql.database}"
        )

    @property
    def newapi_mysql_url(self) -> str:
        return (
            f"mysql+aiomysql://{self.newapi_mysql.user}:{self.newapi_mysql.password}"
            f"@{self.newapi_mysql.host}:{self.newapi_mysql.port}/{self.newapi_mysql.database}"
        )


config = AppConfig()
