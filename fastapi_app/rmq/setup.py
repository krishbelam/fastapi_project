"""
rmq/setup.py
───────────────
Production-grade RabbitMQ setup using aio-pika.

"""

import asyncio
import aio_pika

from fastapi_app.core import settings

from fastapi_app.logger_engine import logger


# ── Consumer loop ─────────────────────────────────────────────────────────────
async def rmq_setup(max_retries=3, msg_from=None) -> None:
    """
    Runs for the entire app lifetime (started as asyncio.create_task in lifespan).

    """
    msg_from = msg_from if msg_from else "RabbitMQ Setup"
    logger.info(msg_from=msg_from, msg=f"📨 RabbitMQ Queue Setup starting, max_retries was set to {max_retries}")

    # Clean up retries
    if not isinstance(max_retries, int) or max_retries <= 0:
        try:
            max_retries = int(max_retries)
            if max_retries < 0:
                max_retries = 1
        except (ValueError, TypeError):
            max_retries = 1

    max_retries = max(int(max_retries), 1) if isinstance(max_retries, (int, str)) else 1

    attempt = 1
    while attempt <= max_retries:
        retry_delay_time = min(attempt * 2, 30)

        connection = None
        try:
            connection = await aio_pika.connect_robust(
                settings.RABBITMQ_URL,
                reconnect_interval=5,
            )
            logger.info(msg_from=msg_from, msg="✅ RabbitMQ setup connected")

            async with connection:
                channel = await connection.channel()

                exchange = await channel.declare_exchange(
                    settings.RMQ_EXCHANGE,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
                queue = await channel.declare_queue(
                    settings.RMQ_QUEUE,
                    durable=True,
                    arguments={"x-dead-letter-exchange": settings.RMQ_DLX},  # ← missing
                )
                await queue.bind(exchange, routing_key=settings.RMQ_ROUTING_KEY)

                logger.info(
                    msg_from=msg_from,
                    msg=f"📨 Setting up the queue='{settings.RMQ_QUEUE}' with Exchange=`{settings.RMQ_EXCHANGE}`"
                )

                break

        # Exception order
        # AMQPChannelError → AMQPConnectionError → CancelledError → Exception
        except aio_pika.exceptions.AMQPChannelError as exc:
            logger.error(
                msg_from=msg_from,
                msg=f"💥 Channel error — queue '{settings.RMQ_QUEUE}' likely deleted: {exc}  "
                    f"retrying in{retry_delay_time}s"
            )
            await asyncio.sleep(retry_delay_time)
            attempt += 1

        except aio_pika.exceptions.AMQPConnectionError as exc:
            logger.warning(msg_from=msg_from,
                           msg=f"🔴 RabbitMQ connection lost: {exc} — retrying in 5{retry_delay_time}")
            await asyncio.sleep(retry_delay_time)
            attempt += 1

        except Exception as exc:
            logger.error(msg_from=msg_from, msg=f"💥 Unexpected error: {exc} — retrying in {retry_delay_time}s")
            await asyncio.sleep(retry_delay_time)
            attempt += 1

        finally:
            # Resource Clean up
            if connection and not connection.is_closed:
                await connection.close()
