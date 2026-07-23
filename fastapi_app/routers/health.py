"""
routers/health.py
─────────────────
Liveness + RabbitMQ health endpoints.
"""

from datetime import datetime

from fastapi import APIRouter

from fastapi_app.rmq import rmq_health
from fastapi_app.logger_engine import logger

router = APIRouter(prefix="/health", tags=["Meta"])


@router.get("/")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@router.get("/rabbitmq")
async def rabbitmq_health():
    """
    Current RabbitMQ health state.

    status values:
      unknown   — app just started, no probe yet
      alive     — last probe succeeded
      dead      — last probe failed (see consecutive_failures)
      recovered — came back after being dead
    """
    logger.debug(
        msg_from="RabbitMQ Health Endpoint",
        msg=f"id(rmq_health)={id(rmq_health)}  status={rmq_health.status}"
    )

    return rmq_health.to_dict()
