"""配置管理 - 通过 .env 文件 / 环境变量加载

变量命名规则：{前缀}_{字段名大写}，如 REDIS_HOST、MYSQL_PASSWORD、ADMIN_AUTH_TOKEN。
优先级：进程环境变量 > .env 文件 > 代码默认值。
Redis / MySQL 均为外部服务，本项目不自带部署。
"""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = dict(
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",  # 忽略 .env 中不属于本配置段的变量
)


class ServerConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SERVER_", **_ENV_FILE)

    host: str = "0.0.0.0"
    port: int = 8080
    workers: int = 4


class NewAPIConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NEWAPI_", **_ENV_FILE)

    base_url: str = "http://127.0.0.1:3000"
    timeout: int = 300


class RedisConfig(BaseSettings):
    """外部 Redis 服务连接配置"""

    model_config = SettingsConfigDict(env_prefix="REDIS_", **_ENV_FILE)

    host: str = "127.0.0.1"
    port: int = 6379
    password: str = ""
    db: int = 0
    pool_size: int = 50


class MySQLConfig(BaseSettings):
    """外部 MySQL（网关自身配置库）连接配置"""

    model_config = SettingsConfigDict(env_prefix="MYSQL_", **_ENV_FILE)

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "ratelimit_gateway"
    pool_size: int = 10


class NewAPIMySQLConfig(BaseSettings):
    """NewAPI 数据库（只读，用于同步 Key->Group 映射）"""

    model_config = SettingsConfigDict(env_prefix="NEWAPI_MYSQL_", **_ENV_FILE)

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "readonly_user"
    password: str = ""
    database: str = "new-api"
    pool_size: int = 5


class SyncConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SYNC_", **_ENV_FILE)

    key_group_interval: int = 60
    config_cache_ttl: int = 60
    key_map_ttl: int = 300


class RateLimitConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RATELIMIT_", **_ENV_FILE)

    on_key_not_found: Literal["passthrough", "reject"] = "passthrough"
    on_config_not_found: Literal["passthrough", "reject"] = "passthrough"
    # Redis 故障时的降级策略：
    #   passthrough   - 放行全部请求（可用性优先，限速失效）
    #   reject        - 拒绝请求返回 503（保护上游优先）
    #   local_fallback- 进程内滑动窗口兜底限速（推荐）
    on_redis_error: Literal["passthrough", "reject", "local_fallback"] = "local_fallback"
    # local_fallback 兜底参数：窗口秒数 + 单 key 最大请求数（每个 worker 独立计数，宜保守）
    fallback_window: int = 60
    fallback_max_requests: int = 60
    # 分组感知兜底：Redis 故障且已解析出分组策略时，按「5h 限额 ÷ 该除数」
    # 执行本地滑动窗口。0 = 自动使用 SERVER_WORKERS。
    fallback_limit_divisor: int = 0
    # 熔断器：Redis 连续失败达阈值后跳过 Redis 调用直接降级，避免逐请求等待超时
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_open_seconds: float = 10.0
    # L1 进程内缓存（keymap/config，Redis → MySQL 读透前的第一级）
    local_cache_size: int = 50000
    local_cache_ttl: int = 60
    local_cache_negative_ttl: int = 10
    # 请求数模式批量配额：一次 EVALSHA 预取 N 个配额本地消耗，Redis QPS 降为 1/N。
    # 多 worker 并发预取的超发上界约为 (workers-1) * batch_size。
    quota_batch_size: int = 10
    show_remaining: bool = True
    # token 模式 + 流式请求时，自动注入 stream_options.include_usage=true
    # 设为 false 如果上游 LLM 不支持此参数
    auto_inject_include_usage: bool = True


class AdminConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ADMIN_", **_ENV_FILE)

    enabled: bool = True
    auth_token: str = "change-me-in-production"


class AppConfig:
    """全局配置单例"""

    def __init__(self):
        self.server = ServerConfig()
        self.newapi = NewAPIConfig()
        self.redis = RedisConfig()
        self.mysql = MySQLConfig()
        self.newapi_mysql = NewAPIMySQLConfig()
        self.sync = SyncConfig()
        self.ratelimit = RateLimitConfig()
        self.admin = AdminConfig()

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
