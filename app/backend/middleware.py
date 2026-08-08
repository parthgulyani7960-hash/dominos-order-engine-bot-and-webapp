"""Request correlation-ID middleware.

Assigns a unique ``X-Correlation-ID`` to every inbound HTTP request so that
all log records produced during that request can be tied together for
distributed tracing.

The ID is stored in three places:
1. ``request.state.correlation_id`` – accessible in endpoint handlers.
2. structlog's per-request ``contextvars`` – automatically included in every
   structlog log record via the ``merge_contextvars`` processor.
3. The ``X-Correlation-ID`` response header – returned to the caller so
   clients can correlate their own logs.
"""

from __future__ import annotations

import uuid
from typing import Callable, Awaitable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from structlog.contextvars import bind_contextvars, clear_contextvars


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Starlette/FastAPI middleware that propagates a correlation ID.

    Accepts ``X-Correlation-ID`` from the caller or generates a new UUID4.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        # 1. Determine the correlation ID for this request.
        correlation_id: str = (
            request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        )

        # 2. Make it accessible in handler code.
        request.state.correlation_id = correlation_id

        # 3. Bind to structlog context so all log records within this request
        #    automatically include the correlation_id field.
        clear_contextvars()
        bind_contextvars(correlation_id=correlation_id)

        # 4. Process the request.
        response: Response = await call_next(request)

        # 5. Echo the ID back so the caller can trace their request.
        response.headers["X-Correlation-ID"] = correlation_id

        # 6. Clear structlog context – prevents leakage to the next request
        #    if the event loop reuses the same task.
        clear_contextvars()

        return response
