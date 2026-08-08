"""Health and readiness probe endpoints.

Designed for use with Kubernetes, Docker Compose, or Cloud Run health checks.

Endpoints
---------
GET /health
    Liveness probe.  Returns 200 as long as the Python process is running.

GET /ready
    Readiness probe.  Attempts lightweight checks against the configured
    PostgreSQL database and Redis server.  Returns 200 only when both
    dependencies are reachable; returns 503 with a diagnostic payload
    otherwise.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from .settings import settings

router = APIRouter(tags=["health"])


# ---------------------------------------------------------------------------
# Liveness
# ---------------------------------------------------------------------------

@router.get("/health", status_code=status.HTTP_200_OK, summary="Liveness probe")
async def health_check() -> Dict[str, Any]:
    """Always returns 200 while the process is alive."""
    return {"status": "healthy", "environment": settings.ENV}


# ---------------------------------------------------------------------------
# Readiness
# ---------------------------------------------------------------------------

async def _check_postgres() -> bool:
    """Attempt a single lightweight SQL query to verify DB connectivity."""
    try:
        import asyncpg  # type: ignore[import]
        conn = await asyncio.wait_for(
            asyncpg.connect(settings.POSTGRES_DSN, timeout=3),
            timeout=5,
        )
        await conn.execute("SELECT 1")
        await conn.close()
        return True
    except Exception:
        return False


async def _check_redis() -> bool:
    """Attempt a PING to verify Redis connectivity."""
    try:
        import redis.asyncio as aioredis  # type: ignore[import]
        client = aioredis.from_url(settings.REDIS_URL, socket_timeout=3)
        await asyncio.wait_for(client.ping(), timeout=5)
        await client.aclose()
        return True
    except Exception:
        return False


@router.get("/ready", summary="Readiness probe")
async def readiness_check() -> JSONResponse:
    """Returns 200 when all critical dependencies are reachable, else 503."""
    pg_ok, redis_ok = await asyncio.gather(_check_postgres(), _check_redis())

    checks: Dict[str, str] = {
        "postgres": "ok" if pg_ok else "unavailable",
        "redis": "ok" if redis_ok else "unavailable",
    }
    all_ok: bool = all(v == "ok" for v in checks.values())

    payload = {
        "status": "ready" if all_ok else "degraded",
        "environment": settings.ENV,
        "checks": checks,
    }
    http_status = status.HTTP_200_OK if all_ok else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(content=payload, status_code=http_status)
