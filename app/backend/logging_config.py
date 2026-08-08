"""Structured logging configuration using structlog.

All logs are emitted as JSON lines and include:
  - ISO timestamp
  - log level
  - correlation_id (if set via structlog.contextvars)
  - full exception / stack info when present

Usage
-----
Call ``configure_logging()`` once at application startup (before any other
import that might log).  Then obtain loggers via::

    import structlog
    logger = structlog.get_logger(__name__)
"""

from __future__ import annotations

import logging
import sys
from typing import Any, MutableMapping

import structlog
from structlog.contextvars import merge_contextvars

from .settings import settings


# ---------------------------------------------------------------------------
# Processors
# ---------------------------------------------------------------------------

def _drop_color_message(
    logger: Any,
    method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Remove the 'color_message' key injected by uvicorn's access logger."""
    event_dict.pop("color_message", None)
    return event_dict


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def configure_logging() -> None:
    """Initialise Python stdlib logging and structlog.

    Must be called once before the FastAPI application begins serving traffic.
    Log level and format are driven by ``settings.LOG_LEVEL``.
    """
    if sys.platform == "win32":
        try:
            if hasattr(sys.stdout, "reconfigure"):
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            if hasattr(sys.stderr, "reconfigure"):
                sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    log_level: int = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    # Configure stdlib root logger so third-party libraries (uvicorn, sqlalchemy, etc.)
    # feed into structlog's pipeline.
    logging.basicConfig(
        level=log_level,
        stream=sys.stdout,
        format="%(message)s",
    )

    # Silence noisy third-party loggers in non-debug environments.
    if log_level > logging.DEBUG:
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)

    shared_processors: list = [
        # Merge any bound context (including correlation_id set by middleware).
        merge_contextvars,
        # Add log level name to the event dict.
        structlog.stdlib.add_log_level,
        # Add logger name.
        structlog.stdlib.add_logger_name,
        # ISO-8601 timestamp.
        structlog.processors.TimeStamper(fmt="iso"),
        # Render exception info as structured dict rather than raw traceback string.
        structlog.processors.format_exc_info,
        structlog.processors.StackInfoRenderer(),
        # Strip uvicorn color codes.
        _drop_color_message,
    ]

    structlog.configure(
        processors=shared_processors + [structlog.processors.JSONRenderer()],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


# ---------------------------------------------------------------------------
# Module-level convenience logger
# ---------------------------------------------------------------------------
# Importers can do:  from .logging_config import logger
logger = structlog.get_logger(__name__)
