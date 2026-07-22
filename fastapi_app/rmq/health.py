"""
rmq/health.py
─────────────
RabbitMQ health checker — distinguishes broker health from queue health.

Key insight: deleting a queue from the UI causes a channel-level AMQP 404,
which RabbitMQ uses to permanently close that channel. Two separate things
can be wrong independently:
  - Broker DOWN  → TCP connection fails
  - Queue MISSING → broker alive but queue deleted

Both are tracked separately in RMQHealthState.
"""
import time
import asyncio
import datetime as dt
import logging
from enum import Enum

import aio_pika
from aio_pika.exceptions import AMQPChannelError

from fastapi_app.core import settings
from .setup import rmq_setup


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Shared channel reference
# ─────────────────────────────────────────────
_shared_channel: aio_pika.RobustChannel | None = None


def set_shared_channel(channel: aio_pika.RobustChannel | None) -> None:
    global _shared_channel
    _shared_channel = channel
    state = "registered" if channel else "cleared"
    logger.debug(f"🩺 Health checker: shared channel {state}")


# ─────────────────────────────────────────────
# State
# ─────────────────────────────────────────────
class RMQStatus(str, Enum):
    UNKNOWN  = "unknown"
    ALIVE    = "alive"    # broker up, queue exists
    DEAD     = "dead"     # broker unreachable
    RECOVERED = "recovered"
    DEGRADED = "degraded" # broker up, but queue missing ← NEW


class RMQHealthState:
    def __init__(self):
        self.status:               RMQStatus      = RMQStatus.UNKNOWN
        self.last_checked:         dt.datetime | None = None
        self.last_alive:           dt.datetime | None = None
        self.last_dead:            dt.datetime | None = None
        self.consecutive_failures: int            = 0
        self.probe_method:         str            = "none"
        self.queue_exists:         bool | None    = None   # None = unknown

    def to_dict(self) -> dict:
        return {
            "status":               self.status,
            "queue_exists":         self.queue_exists,
            "probe_method":         self.probe_method,
            "last_checked":         self.last_checked.isoformat() if self.last_checked else None,
            "last_alive":           self.last_alive.isoformat()   if self.last_alive   else None,
            "last_dead":            self.last_dead.isoformat()    if self.last_dead    else None,
            "consecutive_failures": self.consecutive_failures,
        }


rmq_health = RMQHealthState()


# ─────────────────────────────────────────────
# Event hooks
# ─────────────────────────────────────────────
async def on_rabbitmq_down(state: RMQHealthState) -> None:
    logger.error(
        f"🔴 RabbitMQ DOWN at {state.last_dead.isoformat()} "
        f"(failures: {state.consecutive_failures})"
    )

async def on_rabbitmq_recovered(state: RMQHealthState) -> None:
    logger.info(f"🟢 RabbitMQ RECOVERED at {state.last_alive.isoformat()}")

async def on_rabbitmq_still_alive(state: RMQHealthState) -> None:
    # logger.info(f"✅ RabbitMQ alive via {state.probe_method}")
    logger.debug(f"✅ RabbitMQ alive via {state.probe_method}")
    # pass

async def on_rabbitmq_still_dead(state: RMQHealthState) -> None:
    logger.warning(f"🔴 RabbitMQ still DOWN  failures={state.consecutive_failures}")

async def on_queue_missing(state: RMQHealthState) -> None:
    """
    Broker is alive but the queue was deleted.
    Consumer is effectively broken — alert immediately.
    """
    logger.error(
        f"⚠️  Queue '{settings.RMQ_QUEUE}' MISSING — broker alive but consumer is broken. "
        f"Recreate the queue or redeploy."
    )
    # await notify_slack(f"Queue {settings.RMQ_QUEUE} deleted!")


# ─────────────────────────────────────────────
# Probe result
# ─────────────────────────────────────────────
class ProbeResult:
    def __init__(self, broker_alive: bool, queue_exists: bool | None, method: str):
        self.broker_alive  = broker_alive
        self.queue_exists  = queue_exists   # None when broker is down (can't know)
        self.method        = method


# ─────────────────────────────────────────────
# Probe strategies
# ─────────────────────────────────────────────
async def _probe_passive(channel: aio_pika.RobustChannel, timeout: float) -> ProbeResult:
    """
    Passive declare on existing channel.
    Distinguishes three outcomes:
      - Queue exists           → broker alive, queue_exists=True
      - Queue missing (404)    → broker alive, queue_exists=False  (AMQP 404 = channel closed)
      - Connection/timeout err → broker dead or channel broken
    """
    try:
        """
        Q: 
            Creating the missing queue is a bad Idea?
        
        A:  
            No, it depends on 2 things.
            1 -> In your case, infra team owns it (your words: "configured by some other people") — so your app should never silently recreate it, because you might recreate it with wrong settings (wrong DLX, wrong durability, wrong arguments) and cause subtle data-loss bugs.
            2 -> But if your app owns the queue (you declared it, you manage it), then recreating it on startup is perfectly fine and is actually the standard pattern — just never do it silently on reconnect mid-runtime, only on initial startup.
        """

        # passive=True — assert queue exists, never recreate it.
        # Queue lifecycle is owned by infra (your ops team / Terraform).
        # If your app owns the queue, use passive=False only on initial startup,
        # never on reconnect — wrong settings (DLX, durability) cause silent data loss.
        # queue = await channel.declare_queue(
        #     settings.RMQ_QUEUE,
        #     passive=True,
        #     timeout=timeout
        # )
        await asyncio.wait_for(
            channel.declare_queue(
                settings.RMQ_QUEUE,
                passive=True,
            ),
            timeout=timeout,
        )
        # await channel.declare_queue(settings.RMQ_QUEUE, passive=True, timeout=timeout)
        return ProbeResult(broker_alive=True, queue_exists=True, method="passive")

    except AMQPChannelError as exc:
        # 404 = queue doesn't exist. Broker IS alive — it responded.
        # But the channel is now permanently closed by RabbitMQ protocol.
        logger.warning(f"⚠️  Passive probe got channel error (queue likely deleted): {exc}")
        set_shared_channel(None)   # invalidate — channel is dead
        return ProbeResult(broker_alive=True, queue_exists=False, method="passive")

    except Exception as exc:
        # Timeout, connection closed, network error — broker may be down
        logger.debug(f"Passive probe failed: {type(exc).__name__}: {exc}")
        set_shared_channel(None)   # invalidate untrustworthy channel
        return ProbeResult(broker_alive=False, queue_exists=None, method="passive")


async def _probe_new_connection(timeout: float) -> ProbeResult:
    """
    Slow path: open fresh TCP connection, check queue existence, close.
    Used when no shared channel is available.
    """
    conn = None
    try:
        conn = await aio_pika.connect(settings.RABBITMQ_URL, timeout=timeout)
        # Broker is reachable — now check queue
        channel = await conn.channel()
        try:
            await channel.declare_queue(settings.RMQ_QUEUE, passive=True, timeout=timeout)
            queue_exists = True
        except AMQPChannelError:
            queue_exists = False   # broker responded with 404 — it's alive, queue is gone
        return ProbeResult(broker_alive=True, queue_exists=queue_exists, method="new_connection")

    except Exception as exc:
        logger.debug(f"New-connection probe failed: {type(exc).__name__}: {exc}")
        return ProbeResult(broker_alive=False, queue_exists=None, method="new_connection")

    finally:
        if conn and not conn.is_closed:
            await conn.close()


async def _probe(timeout: float) -> ProbeResult:
    channel = _shared_channel
    if channel is not None and not channel.is_closed:
        return await _probe_passive(channel, timeout)
    return await _probe_new_connection(timeout)


# ─────────────────────────────────────────────
# Health checker loop
# ─────────────────────────────────────────────
async def rmq_health_checker() -> None:
    """
    State machine:
        UNKNOWN   → broker up + queue exists   → ALIVE
        UNKNOWN   → broker up + queue missing  → DEGRADED  fires on_queue_missing()
        UNKNOWN   → broker down                → DEAD      fires on_rabbitmq_down()
        ALIVE     → queue deleted              → DEGRADED  fires on_queue_missing()
        ALIVE     → broker down                → DEAD      fires on_rabbitmq_down()
        DEAD      → broker up + queue exists   → RECOVERED fires on_rabbitmq_recovered()
        DEAD      → broker up + queue missing  → DEGRADED  fires on_queue_missing()
        DEGRADED  → queue recreated            → RECOVERED fires on_rabbitmq_recovered()
    """
    interval      = settings.RMQ_HEALTH_INTERVAL_SECS
    probe_timeout = settings.RMQ_PROBE_TIMEOUT_SECS

    if settings.RMQ_INITIAL_HEALTH_CHECK_TYPE == 'sleep':
        logger.info(f"🩺 [Sleep] Health checker started — delaying health check by {settings.RMQ_HEALTH_INITIAL_DELAY_SECS} s")
        # Option-1: Wait for the initial delay
        # Wait for consumer to finish declaring queue before first probe
        await asyncio.sleep(settings.RMQ_HEALTH_INITIAL_DELAY_SECS)
    else:
        logger.info(f"🩺 [Poll] Health checker started— interval={interval}s  timeout={probe_timeout}s")

        # Option-2: poll until shared channel is available instead of blind sleep:
        # wait for consumer to register channel, max 10s
        for _ in range(20):
            start_time = time.perf_counter()
            if _shared_channel is not None:
                logger.info(
                    f"_shared_channel is set, Time taken : {time.perf_counter() - start_time}. Moving to running the probe"
                )
                break
            await asyncio.sleep(0.5)

    # ── Run first probe immediately on startup ────────────────────────────
    await _run_probe(probe_timeout)   # ← no wait, instant first result

    # ── Then tick on interval ─────────────────────────────────────────────
    while True:
        await asyncio.sleep(interval)
        await _run_probe(probe_timeout)

async def _run_probe(probe_timeout: float) -> None:

    """
    Single probe tick — updates rmq_health state and fires hooks.

    State machine:
        UNKNOWN   → broker up + queue exists   → ALIVE
        UNKNOWN   → broker up + queue missing  → DEGRADED  fires on_queue_missing()
        UNKNOWN   → broker down                → DEAD      fires on_rabbitmq_down()
        ALIVE     → queue deleted              → DEGRADED  fires on_queue_missing()
        ALIVE     → broker down                → DEAD      fires on_rabbitmq_down()
        DEAD      → broker up + queue exists   → RECOVERED fires on_rabbitmq_recovered()
        DEAD      → broker up + queue missing  → DEGRADED  fires on_queue_missing()
        DEGRADED  → queue recreated            → RECOVERED fires on_rabbitmq_recovered()
    """

    logger.debug(f"🩺 Health checker run_probe started — timeout={probe_timeout}s")

    try:
        result      = await _probe(probe_timeout)
        now         = dt.datetime.now(dt.UTC)
        prev_status = rmq_health.status

        rmq_health.last_checked = now
        rmq_health.probe_method = result.method
        rmq_health.queue_exists = result.queue_exists

        if result.broker_alive and result.queue_exists:
            # ── Fully healthy ─────────────────────────────────────
            rmq_health.last_alive           = now
            rmq_health.consecutive_failures = 0

            if prev_status in (RMQStatus.DEAD, RMQStatus.UNKNOWN, RMQStatus.DEGRADED):
                rmq_health.status = RMQStatus.RECOVERED
                await on_rabbitmq_recovered(rmq_health)
            else:
                rmq_health.status = RMQStatus.ALIVE
                await on_rabbitmq_still_alive(rmq_health)

        elif result.broker_alive and not result.queue_exists:
            # ── Broker up, queue missing ──────────────────────────
            rmq_health.consecutive_failures += 1
            if prev_status != RMQStatus.DEGRADED:
                rmq_health.status    = RMQStatus.DEGRADED
                rmq_health.last_dead = now
                await on_queue_missing(rmq_health)
                if settings.CREATE_QUEUE_IF_DELETED:
                    logger.info(
                        f"Queue still missing — failures={rmq_health.consecutive_failures} "
                        "Setting up the RMQ Exchange, Queue again."
                    )
                    await rmq_setup()
                else:
                    logger.info(
                        f"Queue still missing — failures={rmq_health.consecutive_failures} "
                        "Skipping the Setting up the RMQ Exchange, Queue (Due to false in config)."
                    )
            else:
                logger.warning(
                    f"⚠️  Queue still missing — failures={rmq_health.consecutive_failures}"
                )

        else:
            # ── Broker down ───────────────────────────────────────
            rmq_health.consecutive_failures += 1
            if prev_status != RMQStatus.DEAD:
                rmq_health.status    = RMQStatus.DEAD
                rmq_health.last_dead = now
                await on_rabbitmq_down(rmq_health)
            else:
                await on_rabbitmq_still_dead(rmq_health)

    except asyncio.CancelledError:
        logger.info("🩺 Health checker stopped")
        raise

