"""
main.py
───────
Application entry point. Thin by design — all logic lives in submodules.

Structure:
    core/config.py       — pydantic-settings (reads .env)
    core/store.py        — in-memory stores (swap for DB/Redis)
    core/dependencies.py — FastAPI Depends() functions
    core/jobs.py         — background job dispatch (async + sync)
    models/              — Pydantic v2 request/response models
    routers/             — APIRouter modules (users, jobs, async_demo, health)
    rmq/consumer.py      — aio-pika consumer with handler registry
    rmq/health.py        — RabbitMQ health checker (state machine)
    rmq/publisher.py     — async RabbitMQ publisher (lazy connection)
"""

import time
import httpx
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError, OperationalError

from fastapi_app.core import settings, active_connections, broadcast_ws
from fastapi_app.rmq import close as close_publisher, rmq_consumer, rmq_health_checker, rmq_setup
from fastapi_app.routers import async_demo_router, health_router, jobs_router, users_router

from fastapi_app.logger_engine import logger, start_listener, stop_listener


# ─────────────────────────────────────────────
# Custom exceptions
# ─────────────────────────────────────────────
class AppError(Exception):
    def __init__(self, message: str, code: int = 400):
        self.message = message
        self.code = code


# ─────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    msg_from = "App LifeSpan"

    start_listener()
    app_workers = settings.APP_WORKERS

    try:
        app_workers = int(app_workers)
    except (ValueError, TypeError) as e:
        logger.warning(
            msg_from=msg_from,
            msg=f"Failed to evaluate APP_WORKERS, Exception: {e}"
        )
        app_workers = 1

    debug_mode = "ON" if settings.APP_DEBUG else "OFF"
    effective_workers = 1 if settings.APP_DEBUG else app_workers

    logger.info(
        msg_from=msg_from,
        msg=(
            f"🚀 Starting up | "
            f"Configured Workers: {app_workers} | "
            f"Effective Workers: {effective_workers} | "
            f"Debug Mode: {'ON' if settings.APP_DEBUG else 'OFF'}"
        )
    )

    # Shared async HTTP client — one connection pool for the whole process
    app.state.http_client = httpx.AsyncClient(timeout=10.0)

    # To setup the RabbitMQ Exchange, Queue and Binding them
    await rmq_setup(msg_from=msg_from)

    # RabbitMQ consumer — reconnects on broker restart
    # consumer_task = asyncio.create_task(rmq_consumer(), name="mq-consumer")
    consumer_task = asyncio.create_task(rmq_consumer(), name="mq-consumer")

    # RabbitMQ health checker — probes every RMQ_HEALTH_INTERVAL_SECS
    health_task = asyncio.create_task(rmq_health_checker(), name="mq-health")

    # Add this — surfaces silent task crashes immediately
    def _task_error_handler(task: asyncio.Task):
        if not task.cancelled() and task.exception():
            logger.error(f"💥 Task '{task.get_name()}' crashed: {task.exception()}")

    consumer_task.add_done_callback(_task_error_handler)
    health_task.add_done_callback(_task_error_handler)

    logger.info(msg_from=msg_from,
                msg=f"🚀 Starting up — registered exception handlers: {list(app.exception_handlers.keys())}")

    yield  # ← app runs here

    logger.info(msg_from=msg_from, msg="🛑 Shutting down")

    stop_listener()

    for task in (consumer_task, health_task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await close_publisher()
    await app.state.http_client.aclose()


# ─────────────────────────────────────────────
# App
# ─────────────────────────────────────────────
app = FastAPI(
    title="FastAPI Comprehensive Demo",
    version="1.0.0",
    lifespan=lifespan,
)

# ─────────────────────────────────────────────
# Middleware
# ─────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    msg_from = "Req Timing Middleware"
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = (time.perf_counter() - start) * 1000
    logger.info(msg_from=msg_from,
                msg=f"{request.method} {request.url.path} → {response.status_code} ({elapsed:.1f}ms)")
    response.headers["X-Process-Time-Ms"] = f"{elapsed:.1f}"
    return response


# ─────────────────────────────────────────────
# Exception handlers
# ─────────────────────────────────────────────
@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError):
    return JSONResponse(status_code=exc.code, content={"error": exc.message})


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=422, content={"error": str(exc)})


@app.exception_handler(SQLAlchemyTimeoutError)
async def db_timeout_handler(request: Request, exc: SQLAlchemyTimeoutError):
    msg_from = "SQLAlchemy Timeout"
    logger.error(
        msg_from=msg_from,
        msg=(
            f"DB pool exhausted | "
            f"path={request.url.path} | "
            f"method={request.method} | "
            f"client={request.client.host}:{request.client.port} | "
            f"pool_size={settings.DB_POOL_SIZE} | "
            f"max_overflow={settings.MAX_OVERFLOW_SIZE} | "
            f"error={exc}"
        )
    )
    return JSONResponse(
        status_code=503,
        content={"error": "Server busy — too many requests, try again later"},
    )


@app.exception_handler(OperationalError)
async def db_operational_handler(request: Request, exc: OperationalError):
    msg_from = "SQLAlchemy Operational Error"
    logger.error(
        msg_from=msg_from,
        msg=(
            f"DB operational error: {exc} | "
            f"path={request.url.path}"
        )
    )
    return JSONResponse(
        status_code=503,
        content={"error": "Database error — try again"},
    )


# ─────────────────────────────────────────────
# WebSocket
# ─────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """
    Real-time push for job completions and RabbitMQ state changes.
    Background jobs call broadcast_ws() on finish.
    """
    await ws.accept()
    active_connections.append(ws)
    try:
        while True:
            data = await ws.receive_text()
            await ws.send_text(f"echo: {data}")
    except WebSocketDisconnect:
        active_connections.remove(ws)


# ─────────────────────────────────────────────
# Routers
# ─────────────────────────────────────────────
app.include_router(health_router)
app.include_router(users_router)
app.include_router(jobs_router)
app.include_router(async_demo_router)

# main.py
if __name__ == "__main__":
    import uvicorn

    print(f"settings.APP_DEBUG: {settings.APP_DEBUG} | APP_WORKERS: {settings.APP_WORKERS}")
    uvicorn.run(
        "main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=settings.APP_DEBUG,  # True in dev, False in prod
        workers=settings.APP_WORKERS,
    )
