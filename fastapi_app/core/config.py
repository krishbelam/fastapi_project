"""
core/config.py
──────────────
Single source of truth for all config.
Reads from environment variables (or .env file).
"""
import os
import platform
import pathlib

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, model_validator

PLATFORM_LOG_DIR_MAP = {
    "Windows": "C:\\fastapi_app\\logs\\",
    "Linux": "/var/log/fastapi_app",
}
ENV_FILE_PATH = str(pathlib.Path(__file__).parent.parent.parent / ".env")


# ===========================================================================
#               Exception raised as part of Configs Setup
# ===========================================================================
class UnSupportedPlatform(ValueError):
    """This exception is raised when a platform is not supported"""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # env_file=".env",
        env_file=ENV_FILE_PATH,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # ← was "forbid"
    )

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    APP_DEBUG: bool = False
    APP_WORKERS: int = 1  # >1 disables reload
    ROOT_DIR: Optional[str] = None

    APP_RUNNING_PLATFORM: Optional[str] = Field(default=None, description="Platform where the app is running")

    # Database configuration
    # DB_URL: str = Field()  # required — no default, crashes loudly if missing
    # Set defaults to None so Pydantic doesn't crash before the validator runs
    DB_URL: Optional[str] = Field(default=None, description="The primary DB connection string")
    DB_HOST: Optional[str] = Field(default="0.0.0.0")
    # DB_PORT: Optional[int] = Field(default=3306)  # Default MySQL port
    DB_PORT: Optional[int] = Field(default=None)  # To raise exception if port is missing from .env
    DB_USER: Optional[str] = Field(default=None)
    DB_PASSWORD: Optional[str] = Field(default=None)
    DB_NAME: Optional[str] = Field(default=None)  # Corrected type from int to str

    DB_POOL_SIZE: int = Field(default=10)
    MAX_OVERFLOW_SIZE: int = Field(default=20)
    DB_POOL_TIMEOUT: int = Field(default=30)
    POOL_PRE_PING: bool = Field(default=True)
    DB_POOL_RECYCLE_TIME: int = Field(default=1800)

    # ── RabbitMQ ──────────────────────────────
    RABBITMQ_URL: str = Field(default="amqp://guest:guest@localhost:5672/")
    RMQ_QUEUE: str = Field(default="task_queue")
    RMQ_EXCHANGE: str = Field(default="app_exchange")
    RMQ_ROUTING_KEY: str = Field(default="#")  # match all topics
    RMQ_DLX: str = Field(default="dlx")  # dead-letter exchange
    RMQ_PREFETCH_COUNT: int = Field(default=10)

    # ── Concurrency control ────────────────────
    # Rule: RMQ_MAX_CONCURRENT_TASKS >= RMQ_PREFETCH_COUNT
    # prefetch_count  → broker delivers at most N unacked messages
    # max_concurrent  → app processes at most N messages at once (semaphore)
    RMQ_MAX_CONCURRENT_TASKS: int = Field(
        default=50,
        description="App processes at most N messages at once (app-side semaphore)"
    )  # semaphore cap

    # ── Shutdown ───────────────────────────────
    RMQ_DRAIN_TIMEOUT_SECS: float = Field(
        default=30.0,
        description="wait time for in-flight messages on shutdown"
    )  # wait up to N secs for tasks on shutdown

    # ── Health checker ─────────────────────────
    # RMQ_HEALTH_INTERVAL_SECS:  float = 10.0   # probe every N seconds
    RMQ_HEALTH_INTERVAL_SECS: float = Field(default=2.0)  # probe every N seconds
    RMQ_PROBE_TIMEOUT_SECS: float = Field(default=3.0)  # TCP connect timeout
    RMQ_HEALTH_INITIAL_DELAY_SECS: float = Field(
        default=5.0,
        description="RabbitMQ Health Check Initial Interval Delay seconds"
    )
    RMQ_MAX_RETRIES: int = Field(default=3, description="RabbitMQ max retires during the setup")

    RMQ_INITIAL_HEALTH_CHECK_TYPE: Optional[str] = Field(default='poll', description="Initial Health check type")

    CREATE_QUEUE_IF_DELETED: bool = Field(default=False, description="To create the missing queue if we own it")

    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    LOG_DIR: Optional[str] = Field(default=None, description="Log directory")
    LOG_FILE_NAME: str = Field(default="app.log", description="Log file name")
    LOG_FILE_PATH: Optional[str] = Field(default=None, description="Log file full path")
    LOG_DATETIME_FMT: Optional[str] = Field(default=None, description="Datetime format in the Log file")
    ROTATING_FILE_MAX_BYTES: int = Field(default=1024 * 1024 * 5, description="Max File size in MB")
    ROTATING_FILE_BACKUP_COUNT: int = Field(default=5, description="Backup count for rotating files")

    # ── Token Configs ─────────────────────────
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=5, description="Access token expire time in minutes")
    REFRESH_TOKEN_EXPIRE_DAYS: int = Field(default=15, description="Refresh token expire time in days")

    @model_validator(mode="after")
    def validate_or_build_db_url(self) -> "Settings":
        # Scenario A: DB_URL is already provided in environment/.env
        if self.DB_URL:
            print(f"DB_URL: {self.DB_URL} Exists So returning immediately")
            return self

        # Scenario B: DB_URL is missing, so we MUST have the individual parts
        required_fields = {
            "DB_HOST": self.DB_HOST,
            "DB_USER": self.DB_USER,
            "DB_PASSWORD": self.DB_PASSWORD,
            "DB_NAME": self.DB_NAME,
        }

        # Check if any required component is missing
        missing = [key for key, val in required_fields.items() if val is None]
        if missing:
            raise ValueError(
                f"Missing environment variables. Provide either 'DB_URL' or "
                f"all individual components. Missing: {', '.join(missing)}"
            )

        # Build DB_URL dynamically
        self.DB_URL = f"mysql+aiomysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        return self

    @model_validator(mode="after")
    def setup_initials(self) -> "Settings":
        """Validate mandatory fields and evaluate platform-based properties."""

        _platform = platform.system()
        if _platform not in PLATFORM_LOG_DIR_MAP:
            raise UnSupportedPlatform(f"Unsupported platform detected: {_platform}!")

        # Set APP_RUNNING_PLATFORM if missing
        if not self.APP_RUNNING_PLATFORM:
            self.APP_RUNNING_PLATFORM = _platform

        # Set LOG_DIR if missing (Safe to pull since platform is verified above)
        if not self.LOG_DIR:
            self.LOG_DIR = PLATFORM_LOG_DIR_MAP[_platform]

        # Configure the log file path if dependencies exist
        if self.LOG_DIR and self.LOG_FILE_NAME:
            os.makedirs(self.LOG_DIR, exist_ok=True)
            self.LOG_FILE_PATH = os.path.join(self.LOG_DIR, self.LOG_FILE_NAME)

        # Configure the ROOT_DIR
        if not self.ROOT_DIR:
            path = pathlib.Path(os.path.dirname(__file__))
            self.ROOT_DIR = str(path.parent.parent)

        # Configure the Datetime format for Log statement
        if not self.LOG_DATETIME_FMT:
            self.LOG_DATETIME_FMT = "%d-%m-%Y %H:%M:%S"

        return self


settings = Settings()
