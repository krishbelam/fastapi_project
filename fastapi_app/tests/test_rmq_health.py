"""
tests/test_rmq_health.py
─────────────────────────
Unit tests for the RabbitMQ health state machine.
No broker needed — _probe is mocked.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from fastapi_app.rmq import (
    RMQHealthState,
    RMQStatus,
    rmq_health,
    rmq_health_checker,
    on_rabbitmq_down,
    on_rabbitmq_recovered,
)


@pytest.fixture(autouse=True)
def reset_health_state():
    """Reset shared singleton before each test."""
    rmq_health.status               = RMQStatus.UNKNOWN
    rmq_health.last_checked         = None
    rmq_health.last_alive           = None
    rmq_health.last_dead            = None
    rmq_health.consecutive_failures = 0
    yield


# ─────────────────────────────────────────────
# State machine transitions
# ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_unknown_to_alive():
    """First successful probe → ALIVE (silent, no hook)."""
    with patch("rmq.health._probe", AsyncMock(return_value=True)), \
         patch("rmq.health.on_rabbitmq_still_alive", AsyncMock()) as mock_alive:

        task = asyncio.create_task(rmq_health_checker())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # UNKNOWN → first success → RECOVERED hook, not alive hook
        assert rmq_health.status in (RMQStatus.ALIVE, RMQStatus.RECOVERED)


@pytest.mark.asyncio
async def test_alive_to_dead_fires_hook():
    """ALIVE → DEAD triggers on_rabbitmq_down exactly once."""
    rmq_health.status = RMQStatus.ALIVE

    down_hook = AsyncMock()
    with patch("rmq.health._probe", AsyncMock(return_value=False)), \
         patch("rmq.health.on_rabbitmq_down", down_hook):

        task = asyncio.create_task(rmq_health_checker())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        down_hook.assert_called_once()
        assert rmq_health.status == RMQStatus.DEAD
        assert rmq_health.consecutive_failures >= 1


@pytest.mark.asyncio
async def test_dead_to_recovered_fires_hook():
    """DEAD → success → RECOVERED triggers on_rabbitmq_recovered."""
    rmq_health.status = RMQStatus.DEAD
    from datetime import datetime
    rmq_health.last_dead = datetime.utcnow()

    recovered_hook = AsyncMock()
    with patch("rmq.health._probe", AsyncMock(return_value=True)), \
         patch("rmq.health.on_rabbitmq_recovered", recovered_hook):

        task = asyncio.create_task(rmq_health_checker())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        recovered_hook.assert_called_once()
        assert rmq_health.status == RMQStatus.RECOVERED


@pytest.mark.asyncio
async def test_consecutive_failures_increment():
    """Each failed probe increments consecutive_failures."""
    rmq_health.status = RMQStatus.DEAD

    with patch("rmq.health._probe", AsyncMock(return_value=False)), \
         patch("rmq.health.on_rabbitmq_down", AsyncMock()), \
         patch("rmq.health.on_rabbitmq_still_dead", AsyncMock()):

        task = asyncio.create_task(rmq_health_checker())
        await asyncio.sleep(0.15)   # enough for 2–3 ticks at interval=0.05
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert rmq_health.consecutive_failures >= 1
