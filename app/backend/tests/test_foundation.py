"""Unit tests for the Foundation layer.

Covers:
- settings.py  – validation, defaults, feature flags
- logging_config.py – configure_logging() idempotency
- middleware.py – CorrelationIdMiddleware behaviour
- health.py – /health and /ready endpoints

Run with:
    pytest app/backend/tests/test_foundation.py -v
"""

from __future__ import annotations

import os
import asyncio
import uuid
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_minimal_app() -> FastAPI:
    """Return a tiny FastAPI app with only health + correlation middleware."""
    from app.backend.middleware import CorrelationIdMiddleware
    from app.backend.health import router as health_router

    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)
    app.include_router(health_router)
    return app


# ---------------------------------------------------------------------------
# settings.py
# ---------------------------------------------------------------------------

class TestSettings:
    def test_defaults_loaded(self):
        """Settings object can be constructed without a .env file."""
        from app.backend.settings import Settings
        s = Settings()
        assert s.ENV in ("development", "staging", "production")
        assert s.LOG_LEVEL in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    def test_feature_flags_off_by_default(self):
        from app.backend.settings import Settings
        s = Settings()
        assert s.ENABLE_SESSION_MANAGER is False
        assert s.ENABLE_TASK_PROCESSOR is False
        assert s.ENABLE_ORDER_ENGINE is False
        assert s.ENABLE_MENU_SYNC is False
        assert s.ENABLE_PAYMENT_VERIFIER is False
        assert s.ENABLE_ADMIN_DASHBOARD is False

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        from importlib import reload
        import app.backend.settings as _mod
        reload(_mod)
        assert _mod.settings.LOG_LEVEL == "DEBUG"
        # Reload back to normal to avoid polluting other tests
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        reload(_mod)

    def test_invalid_env_raises(self, monkeypatch):
        monkeypatch.setenv("ENV", "invalid_env")
        from importlib import reload
        import app.backend.settings as _mod
        with pytest.raises(Exception):
            from app.backend.settings import Settings
            Settings()
        monkeypatch.delenv("ENV", raising=False)


# ---------------------------------------------------------------------------
# logging_config.py
# ---------------------------------------------------------------------------

class TestLoggingConfig:
    def test_configure_logging_idempotent(self):
        """Calling configure_logging() twice must not raise."""
        from app.backend.logging_config import configure_logging
        configure_logging()
        configure_logging()  # second call – should be a no-op without error

    def test_logger_emits(self):
        from app.backend.logging_config import configure_logging, logger
        configure_logging()
        # Should not raise an exception
        logger.info("test_event", foo="bar")


# ---------------------------------------------------------------------------
# middleware.py
# ---------------------------------------------------------------------------

class TestCorrelationIdMiddleware:
    def setup_method(self):
        self.app = _make_minimal_app()
        self.client = TestClient(self.app, raise_server_exceptions=True)

    def test_generates_correlation_id_when_absent(self):
        resp = self.client.get("/health")
        assert resp.status_code == 200
        assert "x-correlation-id" in resp.headers
        cid = resp.headers["x-correlation-id"]
        # Must be a valid UUID4
        parsed = uuid.UUID(cid, version=4)
        assert str(parsed) == cid

    def test_propagates_existing_correlation_id(self):
        custom_id = str(uuid.uuid4())
        resp = self.client.get("/health", headers={"X-Correlation-ID": custom_id})
        assert resp.headers["x-correlation-id"] == custom_id

    def test_unique_ids_per_request(self):
        ids = {self.client.get("/health").headers["x-correlation-id"] for _ in range(5)}
        assert len(ids) == 5  # All different


# ---------------------------------------------------------------------------
# health.py
# ---------------------------------------------------------------------------

class TestHealthEndpoints:
    def setup_method(self):
        self.app = _make_minimal_app()
        self.client = TestClient(self.app, raise_server_exceptions=True)

    def test_health_returns_200(self):
        resp = self.client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert "environment" in body

    def test_readiness_with_unavailable_deps_returns_503(self):
        """When DB and Redis are unreachable the readiness probe should 503."""
        with (
            patch("app.backend.health._check_postgres", new_callable=AsyncMock, return_value=False),
            patch("app.backend.health._check_redis", new_callable=AsyncMock, return_value=False),
        ):
            resp = self.client.get("/ready")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["checks"]["postgres"] == "unavailable"
        assert body["checks"]["redis"] == "unavailable"

    def test_readiness_with_all_ok_returns_200(self):
        with (
            patch("app.backend.health._check_postgres", new_callable=AsyncMock, return_value=True),
            patch("app.backend.health._check_redis", new_callable=AsyncMock, return_value=True),
        ):
            resp = self.client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
