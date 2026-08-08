"""Centralized application configuration using Pydantic BaseSettings.

All configuration values are validated at startup.  Missing required values
raise a ``ValidationError`` immediately so the application fails fast with a
clear message rather than crashing at runtime.

Environment profiles
--------------------
Set ``ENV=development|staging|production`` in your environment or ``.env`` file.

Feature flags
-------------
All feature flags default to ``False`` (OFF).  Enable them incrementally by
setting the corresponding environment variable to ``true``.

Secrets management
------------------
In production use Docker Secrets or a secrets manager; mount the secret as an
environment variable with the matching name.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal, Mapping

try:
    # Pydantic v2
    from pydantic_settings import BaseSettings  # type: ignore[import]
    from pydantic import Field, field_validator, model_validator, SecretStr
    _PYDANTIC_V2 = True
except ImportError:
    # Pydantic v1 fallback
    from pydantic import BaseSettings, Field, validator, root_validator, SecretStr  # type: ignore[assignment]
    _PYDANTIC_V2 = False


# ---------------------------------------------------------------------------
# Settings model
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    # --- Environment ---
    ENV: Literal["development", "staging", "production"] = Field(
        "development",
        description="Active environment profile.",
    )

    # --- Core connections ---
    POSTGRES_DSN: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/dominos",
        description="SQLAlchemy-compatible PostgreSQL DSN.",
    )
    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL.",
    )

    # --- Secrets ---
    JWT_SECRET: SecretStr = Field(
        default="change-me-in-production",
        description="HMAC key for access JWTs.",
    )
    JWT_REFRESH_SECRET: SecretStr = Field(
        default="change-me-refresh-in-production",
        description="HMAC key for refresh JWTs.",
    )
    ENCRYPTION_KEY: SecretStr = Field(
        default="change-me-encryption-in-production",
        description="AES / Fernet encryption key for sensitive stored data.",
    )

    # --- Feature flags (all OFF by default) ---
    ENABLE_SESSION_MANAGER: bool = Field(False, description="Toggle centralized SessionManager.")
    ENABLE_TASK_PROCESSOR: bool = Field(False, description="Toggle Redis-backed TaskProcessor.")
    ENABLE_ORDER_ENGINE: bool = Field(False, description="Toggle refactored OrderEngine.")
    ENABLE_MENU_SYNC: bool = Field(False, description="Toggle MenuSync scheduler.")
    ENABLE_PAYMENT_VERIFIER: bool = Field(False, description="Toggle PaymentVerifier.")
    ENABLE_ADMIN_DASHBOARD: bool = Field(False, description="Toggle new Admin Dashboard.")

    # --- Operational limits ---
    SESSION_IDLE_TIMEOUT_SECONDS: int = Field(300, ge=60, description="Browser session idle timeout.")
    MAX_CONCURRENT_BROWSERS: int = Field(5, ge=1, description="Upper bound on Playwright browser instances.")
    REQUEST_TTL_SECONDS: int = Field(1800, ge=60, description="Max lifetime for a queued task request.")
    # New configuration variables
    OTP_WAIT_TIMEOUT: int = Field(180, ge=30, description="Timeout waiting for OTP page readiness (seconds).")
    PAGE_RECOVERY_RETRIES: int = Field(3, ge=0, description="Number of retries for recovering a dead page.")
    AUTO_ASSIGN_SESSIONS: bool = Field(True, description="Enable automatic fallback to any active verified session for placing orders.")

    # --- Logging / observability ---
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        "INFO",
        description="Root structlog log level.",
    )
    OTEL_EXPORTER: Literal["none", "console", "otlp"] = Field(
        "none",
        description="OpenTelemetry exporter type.",
    )
    OTEL_ENDPOINT: str = Field(
        "http://localhost:4317",
        description="OTLP gRPC endpoint (used when OTEL_EXPORTER=otlp).",
    )

    # --- Admin / bot ---
    ADMIN_TELEGRAM_ID: str = Field("", description="Telegram ID of the primary admin user.")
    TELEGRAM_BOT_TOKEN: str = Field("", description="Telegram Bot API token.")
    MINI_APP_URL: str = Field("http://localhost:8000", description="Public URL of the mini-app.")

    if _PYDANTIC_V2:
        model_config = {
            "env_file": str(Path(__file__).resolve().parents[2] / ".env"),
            "env_file_encoding": "utf-8",
            "case_sensitive": True,
            "extra": "allow",
        }

        @field_validator("POSTGRES_DSN", "REDIS_URL", mode="before")
        @classmethod
        def non_empty(cls, v: str) -> str:
            if not v or not v.strip():
                raise ValueError("Connection string cannot be empty")
            return v

        @model_validator(mode="after")
        def warn_default_secrets(self) -> "Settings":
            """Warn loudly (or raise in production) if default secret values are used."""
            defaults = {
                "JWT_SECRET": "change-me-in-production",
                "JWT_REFRESH_SECRET": "change-me-refresh-in-production",
                "ENCRYPTION_KEY": "change-me-encryption-in-production",
            }
            import warnings
            for attr, default in defaults.items():
                secret: SecretStr = getattr(self, attr)
                if secret.get_secret_value() == default and self.ENV == "production":
                    raise ValueError(
                        f"{attr} must be overridden in production – do not use the default value."
                    )
                elif secret.get_secret_value() == default:
                    warnings.warn(
                        f"{attr} is still using the default value – override before going to production.",
                        stacklevel=2,
                    )
            return self

    else:
        # Pydantic v1 compatibility
        class Config:
            env_file = str(Path(__file__).resolve().parents[2] / ".env")
            env_file_encoding = "utf-8"
            case_sensitive = True
            extra = "allow"

        @validator("POSTGRES_DSN", "REDIS_URL")
        def non_empty(cls, v: str) -> str:  # type: ignore[misc]
            if not v or not v.strip():
                raise ValueError("Connection string cannot be empty")
            return v

        @root_validator
        def warn_default_secrets(cls, values: Mapping[str, Any]) -> Mapping[str, Any]:  # type: ignore[misc]
            import warnings
            defaults = {
                "JWT_SECRET": "change-me-in-production",
                "JWT_REFRESH_SECRET": "change-me-refresh-in-production",
                "ENCRYPTION_KEY": "change-me-encryption-in-production",
            }
            env = values.get("ENV", "development")
            for attr, default in defaults.items():
                secret = values.get(attr)
                val = secret.get_secret_value() if hasattr(secret, "get_secret_value") else (secret or "")
                if val == default and env == "production":
                    raise ValueError(
                        f"{attr} must be overridden in production – do not use the default value."
                    )
                elif val == default:
                    warnings.warn(
                        f"{attr} is still using the default value – override before going to production.",
                        stacklevel=2,
                    )
            return values


# ---------------------------------------------------------------------------
# Singleton – import everywhere with:  from .settings import settings
# ---------------------------------------------------------------------------
settings = Settings()
