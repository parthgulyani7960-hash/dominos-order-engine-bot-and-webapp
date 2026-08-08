"""Redis-backed queue processor with prioritisation, dead letter queues (DLQ), idempotency, and retries.

This module replaces in-memory asyncio.Queue structures with a robust, Redis-backed
task queue. It ensures task execution durability, visibility, and scalability.
"""

from __future__ import annotations

import asyncio
import json
import uuid
import time
from typing import Any, Callable, Dict, Optional
import structlog
from redis.asyncio import Redis, from_url

logger = structlog.get_logger(__name__)

class TaskProcessor:
    def __init__(
        self,
        redis_url: str,
        queue_name: str = "order_tasks",
        max_retries: int = 3,
        backoff_base: float = 2.0,
    ):
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.dlq_name = f"{queue_name}:dlq"
        self.idempotency_prefix = f"{queue_name}:idemp:"
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.redis: Optional[Redis] = None
        self._running = False
        self._handlers: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
        self._worker_task: Optional[asyncio.Task] = None

    async def connect(self):
        if not self.redis:
            self.redis = from_url(self.redis_url, decode_responses=True)
            logger.info("task_queue_connected", queue_name=self.queue_name)

    async def disconnect(self):
        if self.redis:
            await self.redis.aclose()
            self.redis = None
            logger.info("task_queue_disconnected", queue_name=self.queue_name)

    def register_handler(self, task_type: str, handler: Callable[[Dict[str, Any]], Any]):
        self._handlers[task_type] = handler
        logger.info("task_handler_registered", task_type=task_type)

    async def enqueue(
        self,
        task_type: str,
        payload: Dict[str, Any],
        priority: int = 0,  # Higher is higher priority
        idempotency_key: Optional[str] = None,
        ttl_seconds: int = 1800,
    ) -> bool:
        """Enqueue a task with optional priority, idempotency, and TTL."""
        await self.connect()
        assert self.redis is not None

        # Check idempotency key
        if idempotency_key:
            key = f"{self.idempotency_prefix}{idempotency_key}"
            is_new = await self.redis.set(key, "pending", ex=ttl_seconds, nx=True)
            if not is_new:
                logger.warning("duplicate_task_ignored", idempotency_key=idempotency_key)
                return False

        task_id = str(uuid.uuid4())
        task_data = {
            "id": task_id,
            "type": task_type,
            "payload": payload,
            "retries": 0,
            "created_at": time.time(),
            "idempotency_key": idempotency_key,
        }

        # Score is priority (negated for ascending Redis zset sorting)
        score = -priority
        await self.redis.zadd(self.queue_name, {json.dumps(task_data): score})
        logger.info("task_enqueued", task_id=task_id, task_type=task_type, priority=priority)
        return True

    async def start_worker(self):
        """Start the background task processing worker loop."""
        await self.connect()
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("task_worker_started", queue=self.queue_name)

    async def stop_worker(self):
        """Stop the background worker loop gracefully."""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
        logger.info("task_worker_stopped", queue=self.queue_name)

    async def _worker_loop(self):
        while self._running:
            try:
                assert self.redis is not None
                # Pop next task with highest priority (lowest score)
                tasks = await self.redis.zrange(self.queue_name, 0, 0)
                if not tasks:
                    await asyncio.sleep(0.5)
                    continue

                raw_task = tasks[0]
                task_data = json.loads(raw_task)
                
                # Atomically remove task from queue to claim it
                removed = await self.redis.zrem(self.queue_name, raw_task)
                if not removed:
                    continue  # Claimed by another worker

                logger.info("task_claimed", task_id=task_data["id"], task_type=task_data["type"])
                await self._process_task(task_data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("worker_loop_error", error=str(e))
                await asyncio.sleep(1.0)

    async def _process_task(self, task_data: Dict[str, Any]):
        task_id = task_data["id"]
        task_type = task_data["type"]
        payload = task_data["payload"]
        handler = self._handlers.get(task_type)

        if not handler:
            logger.error("handler_not_found", task_type=task_type)
            await self._move_to_dlq(task_data, "No handler registered")
            return

        try:
            # Propagate task-specific logging contexts
            log = logger.bind(task_id=task_id, task_type=task_type)
            if asyncio.iscoroutinefunction(handler):
                await handler(payload)
            else:
                handler(payload)
            
            # Update idempotency key status to completed
            if task_data.get("idempotency_key") and self.redis:
                key = f"{self.idempotency_prefix}{task_data['idempotency_key']}"
                await self.redis.set(key, "completed", ex=86400) # Keep for 24h
            log.info("task_success")
        except Exception as e:
            logger.error("task_execution_failed", task_id=task_id, error=str(e))
            await self._handle_failure(task_data, str(e))

    async def _handle_failure(self, task_data: Dict[str, Any], error_msg: str):
        retries = task_data["retries"]
        if retries < self.max_retries:
            task_data["retries"] += 1
            # Exponential backoff delay
            delay = self.backoff_base ** retries
            logger.info("task_retry_scheduled", task_id=task_data["id"], attempt=task_data["retries"], delay=delay)
            
            # Non-blocking backoff: run the delay and zadd in a background task
            async def _delayed_retry():
                await asyncio.sleep(delay)
                if self.redis:
                    try:
                        await self.redis.zadd(self.queue_name, {json.dumps(task_data): 0})
                    except Exception as re_err:
                        logger.error("task_re_enqueue_failed", task_id=task_data["id"], error=str(re_err))
            asyncio.create_task(_delayed_retry())
        else:
            await self._move_to_dlq(task_data, f"Max retries reached. Error: {error_msg}")

    async def _move_to_dlq(self, task_data: Dict[str, Any], reason: str):
        assert self.redis is not None
        task_data["dlq_reason"] = reason
        task_data["failed_at"] = time.time()
        await self.redis.rpush(self.dlq_name, json.dumps(task_data))
        logger.error("task_moved_to_dlq", task_id=task_data["id"], reason=reason)
        # Update idempotency status to failed
        if task_data.get("idempotency_key"):
            key = f"{self.idempotency_prefix}{task_data['idempotency_key']}"
            await self.redis.set(key, "failed", ex=3600)
