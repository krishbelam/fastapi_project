"""
rmq/consumer.py
───────────────
Production-grade RabbitMQ consumer using aio-pika.

Safety brakes added:
- asyncio.Semaphore caps concurrent in-flight tasks (prevents OOM)
- prefetch_count tells RabbitMQ not to push more than N unacked messages
- TaskGroup tracks every live task so shutdown is clean (no orphans)
- Semaphore + prefetch_count must be tuned together (see note below)

Why queue.get() instead of queue.iterator()?
  queue.iterator() hangs forever when a queue is deleted mid-consume — RabbitMQ
  sends a channel-close frame but aio-pika's robust connection absorbs it silently.
  queue.get() raises AMQPChannelError on the next poll, which bubbles to the
  except block, triggers rmq_setup(), and recreates the queue.
"""

import asyncio
import json
from typing import Callable, Awaitable

import aio_pika
from aio_pika import IncomingMessage
from aio_pika.exceptions import QueueEmpty

from fastapi_app.core import settings
from fastapi_app.rmq.health import set_shared_channel, rmq_health, RMQStatus
from fastapi_app.rmq.setup import rmq_setup
from logger_engine import logger



MessageHandler = Callable[[dict], Awaitable[None]]

# ── Handler registry ──────────────────────────────────────────────────────────
_handlers: dict[str, MessageHandler] = {}


def register_handler(event_type: str):
    """Decorator to register an async handler for a given event_type."""
    def decorator(fn: MessageHandler):
        _handlers[event_type] = fn
        logger.info(f"📋 Registered handler for event_type='{event_type}'")
        return fn
    return decorator


# ── Example handlers ──────────────────────────────────────────────────────────
@register_handler("user.created")
async def handle_user_created(data: dict) -> None:
    logger.info(f"[user.created] {data}")
    await asyncio.sleep(0.1)


@register_handler("job.requested")
async def handle_job_requested(data: dict) -> None:
    logger.info(f"[job.requested] {data}")
    await asyncio.sleep(0.2)


# ── Semaphore ─────────────────────────────────────────────────────────────────
# Hard cap on concurrent in-flight message tasks.
#
# Rule of thumb:  semaphore >= prefetch_count
#   prefetch_count tells RabbitMQ: "send me at most N unacked messages"
#   semaphore tells your app:      "process at most N messages at once"
#
# If semaphore < prefetch_count, RabbitMQ delivers more messages than you can
# start processing → they queue up in memory, defeating the purpose.
# Keep them equal, or semaphore slightly higher to absorb micro-bursts.
#
# Example: prefetch_count=50, MAX_CONCURRENT=50
#   → RabbitMQ delivers ≤50 messages
#   → your app processes ≤50 at once
#   → memory usage is bounded, DB gets ≤50 concurrent queries
#
_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    """
    Lazy-initialise so the semaphore is created inside the running event loop.
    Never create asyncio primitives at module import time.
    """
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(settings.RMQ_MAX_CONCURRENT_TASKS)
        logger.info(f"🔒 Semaphore initialised: max={settings.RMQ_MAX_CONCURRENT_TASKS}")
    return _semaphore

# ================================================================================
#           Old Approach with: async with message.process(requeue=False)
# ================================================================================
# # ── Core message processor ────────────────────────────────────────────────────
# async def _process_message(message: IncomingMessage) -> None:
#     """
#     Wraps handler execution with the semaphore.
#
#     Flow:
#         acquire semaphore slot   ← blocks here if at capacity
#           parse JSON
#           route to handler
#           ack on success
#           nack+requeue on handler error
#           nack+drop  on bad JSON / unknown type
#         release semaphore slot   ← always, even on exception
#     """
#     semaphore = _get_semaphore()
#
#     async with semaphore:                               # ← SAFETY BRAKE
#         active = settings.RMQ_MAX_CONCURRENT_TASKS - semaphore._value
#         logger.debug(f"⚙️  Slot acquired — active tasks: {active}/{settings.RMQ_MAX_CONCURRENT_TASKS}")
#
#         async with message.process(requeue=False):
#             # ── Parse ─────────────────────────────────────────────────
#             try:
#                 body = json.loads(message.body.decode())
#             except (json.JSONDecodeError, UnicodeDecodeError) as exc:
#                 logger.error(f"💥 Bad JSON (dropping): {exc}  raw={message.body[:120]}")
#                 return                                  # nack, requeue=False
#
#             event_type = body.get("event_type", "")
#             handler    = _handlers.get(event_type)
#
#             # ── Route ─────────────────────────────────────────────────
#             if handler is None:
#                 logger.warning(f"⚠️  No handler for '{event_type}' — dropping")
#                 return                                  # nack, requeue=False
#
#             # ── Execute ───────────────────────────────────────────────
#             try:
#                 await handler(body)                     # ← your business logic
#             except Exception as exc:
#                 logger.error(f"💥 Handler '{event_type}' raised: {exc} — requeueing")
#                 await message.nack(requeue=True)
#                 raise
# ================================================================================


# ── Core message processor ────────────────────────────────────────────────────
async def _process_message(message: IncomingMessage) -> None:
    """
    Wraps handler execution with the semaphore.

    Flow:
        acquire semaphore slot   ← blocks here if at capacity
          parse JSON
          route to handler
          ack on success
          nack+requeue on handler error
          nack+drop  on bad JSON / unknown type
        release semaphore slot   ← always, even on exception
    """
    semaphore = _get_semaphore()

    async with semaphore:                               # ← SAFETY BRAKE
        active = settings.RMQ_MAX_CONCURRENT_TASKS - semaphore._value
        logger.debug(f"⚙️  Slot acquired — active tasks: {active}/{settings.RMQ_MAX_CONCURRENT_TASKS}")

        async with message.process(requeue=False):
            # ── Parse ─────────────────────────────────────────────────
            try:
                body = json.loads(message.body.decode())
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error(f"💥 Bad JSON (dropping): {exc}  raw={message.body[:120]}")
                return                                  # nack, requeue=False

            event_type = body.get("event_type", "")
            handler    = _handlers.get(event_type)

            # ── Route ─────────────────────────────────────────────────
            if handler is None:
                logger.warning(f"⚠️  No handler for '{event_type}' — dropping")
                return                                  # nack, requeue=False

            # ── Execute ───────────────────────────────────────────────
            try:
                await handler(body)                     # ← your business logic
            except Exception as exc:
                logger.error(f"💥 Handler '{event_type}' raised: {exc} — requeueing")
                await message.nack(requeue=True)
                raise


# ── Task tracker ──────────────────────────────────────────────────────────────
# Keeps strong references to all live tasks.
# Without this, the GC can collect a task mid-execution (Python 3.12+ warning).
_active_tasks: set[asyncio.Task] = set()


def _spawn_task(message: IncomingMessage) -> None:
    """
    Creates a tracked task for _process_message.
    The task removes itself from _active_tasks when done.
    """
    logger.debug(f"📩 Spawning task for message: {message.body[:100]}")  # ← add

    task = asyncio.create_task(
        _process_message(message),
        name=f"msg-{message.message_id or id(message)}",
    )
    _active_tasks.add(task)
    task.add_done_callback(_active_tasks.discard)       # auto-cleanup on finish


async def _drain_tasks(timeout: float = 30.0) -> None:
    """
    Wait for all in-flight tasks to finish before shutdown.
    Gives handlers time to ack their messages cleanly.
    """
    if not _active_tasks:
        return
    logger.info(f"⏳ Draining {len(_active_tasks)} in-flight tasks (timeout={timeout}s)")
    try:
        await asyncio.wait_for(
            asyncio.gather(*_active_tasks, return_exceptions=True),
            timeout=timeout,
        )
        logger.info("✅ All tasks drained")
    except asyncio.TimeoutError:
        remaining = len(_active_tasks)
        logger.warning(f"⚠️  Drain timeout — {remaining} tasks still running, cancelling")
        for task in list(_active_tasks):
            task.cancel()


# ── Consumer loop ─────────────────────────────────────────────────────────────
async def rmq_consumer(retry_delay_time=10) -> None:
    """
    Runs for the entire app lifetime (started as asyncio.create_task in lifespan).

    Safety layers in order:
        1. prefetch_count   → RabbitMQ delivers at most N unacked messages
        2. Semaphore        → app processes at most N messages concurrently
        3. Task tracker     → no orphaned tasks; clean drain on shutdown
        4. connect_robust   → auto-reconnects on transient drops
        5. while True       → re-connects after hard broker failures
    """
    logger.info(f"📨 RabbitMQ consumer starting, retry_delay_time was set to {retry_delay_time}")

    while True:
        connection = None
        try:
            connection = await aio_pika.connect_robust(
                settings.RABBITMQ_URL,
                reconnect_interval=5,
            )
            logger.info("✅ RabbitMQ consumer connected")

            async with connection:
                channel = await connection.channel()
                set_shared_channel(channel)   # health checker uses this for passive probes

                # prefetch_count ← first safety brake (broker side)
                # Must match or be <= RMQ_MAX_CONCURRENT_TASKS (semaphore)
                await channel.set_qos(prefetch_count=settings.RMQ_PREFETCH_COUNT)

                exchange = await channel.declare_exchange(
                    settings.RMQ_EXCHANGE,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )

                # passive=True — assert queue exists, never recreate it.
                # Queue lifecycle is owned by infra (your ops team / Terraform).
                # If the queue is deleted, fail loudly so health checker catches it.
                queue = await channel.declare_queue(
                    settings.RMQ_QUEUE,
                    passive=True,   # ← raises AMQPChannelError (404) if queue missing
                )
                await queue.bind(exchange, routing_key=settings.RMQ_ROUTING_KEY)

                logger.info(
                    f"📨 Consuming  queue='{settings.RMQ_QUEUE}'  "
                    f"prefetch={settings.RMQ_PREFETCH_COUNT}  "
                    f"max_concurrent={settings.RMQ_MAX_CONCURRENT_TASKS}"
                )
                #
                # async with queue.iterator() as messages:
                #     async for message in messages:
                #         _spawn_task(message)
                #     logger.info("📨 Iterator exited")  # ← add this

                # ── Poll loop (replaces queue.iterator()) ─────────────────
                # queue.iterator() hangs forever on queue deletion.
                # queue.get() raises AMQPChannelError when queue is gone —
                # which bubbles to the except block below.
                while True:
                    try:
                        message = await queue.get(timeout=5, no_ack=False)
                        _spawn_task(message)
                    except QueueEmpty:
                        # No messages right now — yield to event loop and retry
                        await asyncio.sleep(0.1)
                    except asyncio.TimeoutError:
                        # get() timed out — queue exists, just empty
                        await asyncio.sleep(0.1)


        # Exception order
        # AMQPChannelError → AMQPConnectionError → CancelledError → Exception
        except aio_pika.exceptions.AMQPChannelError as exc:
            # Queue was deleted (404) or channel closed by broker.
            # Flag DEGRADED immediately so health checker reflects it
            # even before its next tick fires.
            rmq_health.status = RMQStatus.DEGRADED
            set_shared_channel(None)
            logger.error(
                f"💥 Channel error — queue '{settings.RMQ_QUEUE}' likely deleted: {exc}  "
                f"retrying in{retry_delay_time}s"
            )
            await rmq_setup()  # <--- Recreates the missing Queue
            await asyncio.sleep(retry_delay_time)

        except aio_pika.exceptions.AMQPConnectionError as exc:
            logger.warning(f"🔴 RabbitMQ connection lost: {exc} — retrying in 5{retry_delay_time}")
            set_shared_channel(None)
            await asyncio.sleep(retry_delay_time)


        except asyncio.CancelledError:
            logger.info("📨 Consumer cancelled — draining in-flight tasks")
            set_shared_channel(None)
            await _drain_tasks(timeout=settings.RMQ_DRAIN_TIMEOUT_SECS)
            if connection and not connection.is_closed:
                await connection.close()
            raise

        except Exception as exc:
            logger.error(f"💥 Unexpected error: {exc} — retrying in {retry_delay_time}s")
            set_shared_channel(None)
            await asyncio.sleep(retry_delay_time)
