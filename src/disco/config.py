import pickle
from functools import lru_cache
from typing import Any, Literal, cast

from pydantic import BaseModel, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigError(RuntimeError):
    """Configuration-related error."""
    pass


# ─────────────────────────────────────────────────────────────
# Section configs
# ─────────────────────────────────────────────────────────────


class LoggingSettings(BaseModel):
    level: Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"] = "INFO"
    format: str = (
        "%(asctime)-20s %(processName)-30s %(name)-40s "
        "%(levelname)-8s: %(message)s"
    )


class GrpcSettings(BaseModel):
    bind_host: str = Field(
        "0.0.0.0", description="Host/interface to bind the gRPC server to."
    )
    bind_port: int = Field(
        50051, description="Port to bind the gRPC server to."
    )

    timeout_s: float = Field(
        600.0, description="Default timeout for gRPC calls in seconds."
    )
    max_workers: int = Field(
        10, description="Maximum number of worker threads for the gRPC server."
    )
    grace_s: float = Field(
        60.0, description="Grace period in seconds for server shutdown."
    )

    max_send_message_bytes: int | None = Field(
        default=None,
        description="Maximum send message size in bytes (None = default).",
    )
    max_receive_message_bytes: int | None = Field(
        default=None,
        description="Maximum receive message size in bytes (None = default).",
    )

    keepalive_time_s: float | None = Field(
        default=None,
        description="Time between keepalive pings in seconds (None = disabled).",
    )
    keepalive_timeout_s: float | None = Field(
        default=None,
        description="Timeout for keepalive pings in seconds (None = default).",
    )
    keepalive_permit_without_calls: bool = Field(
        False,
        description="Allow keepalive pings even when there are no active calls.",
    )

    compression: Literal["none", "gzip"] = Field(
        "none",
        description="Compression algorithm for gRPC calls.",
    )


class ZookeeperSettings(BaseModel):
    hosts: str = Field(
        "localhost:2181",
        description="Comma-separated host:port pairs for Zookeeper ensemble.",
    )
    chroot: str | None = Field(
        default=None,
        description="Optional chroot path, e.g. /disco.",
    )

    default_group: str = Field(
        "default",
        description="Default logical group / namespace used in the app.",
    )

    session_timeout_s: float = Field(
        10.0,
        description="Zookeeper session timeout in seconds.",
    )
    connection_timeout_s: float = Field(
        5.0,
        description="Initial connection timeout in seconds.",
    )

    max_retries: int = Field(
        5,
        description="Maximum number of retry attempts for failed operations.",
    )
    retry_delay_s: float = Field(
        1.0,
        description="Delay between retries in seconds.",
    )

    auth_scheme: str | None = Field(
        default=None,
        description="Optional auth scheme, e.g. 'digest'.",
    )
    auth_credentials: str | None = Field(
        default=None,
        description="Optional auth credentials, e.g. 'user:password'.",
    )

    use_tls: bool = Field(
        False,
        description="Enable TLS/SSL for Zookeeper connection.",
    )
    ca_cert: str | None = Field(
        default=None,
        description="Path to CA certificate file for TLS, if applicable.",
    )
    client_cert: str | None = Field(
        default=None,
        description="Path to client certificate file for mutual TLS.",
    )
    client_key: str | None = Field(
        default=None,
        description="Path to client private key for mutual TLS.",
    )


class DatabaseSettings(BaseModel):
    """
    DB config as a SQLAlchemy URL.

    In production, override via:
    - env var:     DISCO_DATABASE__URL
    - dotenv:      .env / .env.local
    - secret file: /run/secrets/disco/database__url
    """
    url: PostgresDsn = Field(
        cast(PostgresDsn, "postgresql+psycopg://user:password@localhost:5432/disco"),
        description="SQLAlchemy-style database URL.",
    )
    pool_size: int = 10
    max_overflow: int = 20


class SerializationSettings(BaseModel):
    """
    Serialization settings for pickle usage.

    protocol:
        - 0–5 in Python 3.8
        - 0–6 in Python 3.12+
    Default: highest available protocol for the running interpreter.
    """

    protocol: int = Field(
        pickle.HIGHEST_PROTOCOL,
        description=(
            "Pickle protocol version. Defaults to the highest supported "
            "by this Python version."
        ),
    )

    def validate_protocol(self) -> None:
        """Ensure user-specified protocol is supported by this interpreter."""
        import pickle as _pickle

        if not (0 <= self.protocol <= _pickle.HIGHEST_PROTOCOL):
            raise ValueError(
                f"Unsupported pickle protocol={self.protocol}. "
                f"This interpreter supports up to protocol={_pickle.HIGHEST_PROTOCOL}."
            )


# ─────────────────────────────────────────────────────────────
# Top-level settings
# ─────────────────────────────────────────────────────────────


class AppSettings(BaseSettings):
    """
    Canonical application configuration for a disco-based service.

    Precedence (highest → lowest):

    1. Init kwargs (tests/overrides)
    2. Environment variables
    3. .env and .env.local
    4. Secret files in /run/secrets/disco
    5. Defaults in this class
    """

    model_config = SettingsConfigDict(
        env_prefix="DISCO_",  # DISCO_LOGGING__LEVEL, DISCO_DATABASE__URL, ...
        env_file=(".env", ".env.local"),  # .env.local overrides .env
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        secrets_dir="/run/secrets/disco",
        extra="ignore",
        validate_default=True,
    )

    app_name: str = "disco"
    debug: bool = False

    logging: LoggingSettings = LoggingSettings()
    grpc: GrpcSettings = GrpcSettings()  # type: ignore[call-arg]
    zookeeper: ZookeeperSettings = ZookeeperSettings()  # type: ignore[call-arg]
    database: DatabaseSettings = DatabaseSettings()  # type: ignore[call-arg]
    serialization: SerializationSettings = SerializationSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_settings(**overrides: Any) -> AppSettings:
    """
    Cached accessor for process-wide settings.

    `overrides` are init kwargs → highest precedence (handy in tests).
    """
    settings = AppSettings(**overrides)
    settings.serialization.validate_protocol()
    return settings
