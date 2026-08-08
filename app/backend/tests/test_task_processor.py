"""Unit and integration tests for Phase 4: TaskProcessor.

Covers:
- Enqueueing and worker loops
- Priorities and sorting
- Idempotency deduplication
- DLQ routing and retries
"""

import asyncio
import pytest
from app.backend.services.task_processor import TaskProcessor

# We use redislite or standard mock if a real Redis server isn't running,
# but for local unit tests, we'll connect to the default localhost Redis
# or skip/mock if unavailable.
REDIS_URL = "redis://localhost:6379/1"

@pytest.mark.asyncio
async def test_task_processor_flow():
    processor = TaskProcessor(redis_url=REDIS_URL, queue_name="test_tasks")
    
    # Try to connect. If local redis is offline, skip test gracefully.
    try:
        await processor.connect()
        assert processor.redis is not None
        await processor.redis.ping()
    except Exception:
        pytest.skip("Local Redis server is not running on port 6379")

    try:
        # Clear existing keys
        await processor.redis.flushdb()

        processed_payloads = []
        
        async def dummy_handler(payload):
            processed_payloads.append(payload)

        processor.register_handler("test_job", dummy_handler)
        
        # Test Priority sorting (higher priority enqueued second should execute first)
        await processor.enqueue("test_job", {"index": 1}, priority=1)
        await processor.enqueue("test_job", {"index": 2}, priority=10) # Higher priority

        # Run worker loop briefly
        await processor.start_worker()
        await asyncio.sleep(1.0)
        await processor.stop_worker()

        assert len(processed_payloads) == 2
        # Index 2 must be processed first because of priority=10 vs priority=1
        assert processed_payloads[0]["index"] == 2
        assert processed_payloads[1]["index"] == 1

        # Test Idempotency key
        processed_payloads.clear()
        res1 = await processor.enqueue("test_job", {"index": 3}, idempotency_key="unique_key")
        res2 = await processor.enqueue("test_job", {"index": 3}, idempotency_key="unique_key")
        
        assert res1 is True
        assert res2 is False # Duplicate key rejected

    finally:
        await processor.disconnect()

@pytest.mark.asyncio
async def test_task_processor_dlq():
    processor = TaskProcessor(redis_url=REDIS_URL, queue_name="test_dlq_tasks", max_retries=1, backoff_base=1.0)
    
    try:
        await processor.connect()
        assert processor.redis is not None
        await processor.redis.ping()
    except Exception:
        pytest.skip("Local Redis server is not running on port 6379")

    try:
        await processor.redis.flushdb()

        async def failing_handler(payload):
            raise ValueError("Processing failed")

        processor.register_handler("failing_job", failing_handler)
        await processor.enqueue("failing_job", {"test": "data"})

        # Run worker
        await processor.start_worker()
        await asyncio.sleep(2.5) # Wait for execution + 1 retry
        await processor.stop_worker()

        # Check DLQ
        dlq_length = await processor.redis.llen(processor.dlq_name)
        assert dlq_length == 1
        dlq_item = await processor.redis.lpop(processor.dlq_name)
        assert dlq_item is not None
        task_data = json_loads_compat(dlq_item)
        assert task_data["payload"]["test"] == "data"
        assert "Max retries reached" in task_data["dlq_reason"]

    finally:
        await processor.disconnect()

def json_loads_compat(val):
    import json
    return json.loads(val)
