# fastapi-demo

A production-structured FastAPI application covering every core concept:
DI, Pydantic v2, Lifespan, Middleware, Background Jobs (async + sync),
RabbitMQ consumer + publisher + health checker, WebSocket, and tests.

---

## Project structure

```
fastapi_project/          ← project root
├── alembic/              ✅ stays here
│   ├── env.py
│   └── versions/
├── alembic.ini           ✅ stays here
├── pyproject.toml
├── fastapi_app/          ← your application package
│    ├── main.py                      ← thin entry point: lifespan, middleware, WS, routers
│    │
│    ├── core/
│    │   ├── config.py                ← pydantic-settings (reads .env)
│    │   ├── database.py              ← async engine, AsyncSession, get_db() dependency
│    │   ├── dependencies.py          ← all Depends() functions
│    │   ├── jobs.py                  ← background job dispatch (async + sync via to_thread)
│    │   └── store.py                 ← WS broadcaster (swap job_status_store → Redis)
│    │
│    ├── models/
│    │   ├── user.py                  ← UserCreate, UserResponse (Pydantic v2)
│    │   ├── job.py                   ← JobRequest, JobStatusResponse
│    │   └── orm/
│    │       ├── base.py              ← DeclarativeBase
│    │       ├── user.py              ← User SQLAlchemy mapped class
│    │       └── job.py               ← Job SQLAlchemy mapped class
│    │
│    ├── services/
│    │   ├── user_service.py          ← DB logic for users (create, get, list, delete)
│    │   └── job_service.py           ← DB logic for jobs (submit, poll, update status)
│    │
│    ├── routers/
│    │   ├── users.py                 ← CRUD → user_service → DB · publishes RMQ events
│    │   ├── jobs.py                  ← submit + poll background jobs
│    │   ├── async_demo.py            ← gather, create_task, to_thread, sleep demos
│    │   └── health.py                ← /health · /health/rabbitmq
│    │
│    ├── auth/
│    │   ├── jwt.py                   ← PyJWT create/decode access tokens
│    │   ├── refresh.py               ← refresh token rotation + revocation
│    │   └── dependencies.py          ← get_current_user via OAuth2PasswordBearer
│    │
│    ├── rmq/
│    │   ├── consumer.py              ← aio-pika · passive=True · semaphore · task drain
│    │   ├── health.py                ← UNKNOWN/ALIVE/DEAD/DEGRADED/RECOVERED state machine
│    │   └── publisher.py             ← lazy robust connection · persistent delivery
│    │
│    ├── alembic/
│    │   ├── env.py                   ← async migration runner (asyncio.run + run_sync)
│    │   └── versions/                ← auto-generated migration scripts
│    │
│    └── tests/
│        ├── conftest.py
│        ├── test_users.py
│        ├── test_jobs.py
│        └── test_rmq_health.py       ← state machine unit tests, no broker needed
```

---

## Quickstart

```bash
# 1. copy and edit config
cp .env.example .env

# 2. install
uv sync

# 3. run (RabbitMQ optional — app starts without it, health status = dead)
uv run uvicorn main:app --reload

# 4. open interactive docs
open http://localhost:9000/docs
```

## 📦 Project-Based Commands (Recommended)

Use these when managing your environment via pyproject.toml and uv.lock.

```bash
uv sync                      # Install all standard project dependencies
uv sync --all-groups         # Install everything including development & test tools
uv sync --clean              # Install valid dependencies and UNINSTALL everything else
uv add fastapi               # Add a package to pyproject.toml and install it instantly
uv remove fastapi            # Remove a package from pyproject.toml and uninstall it
uv run python main.py        # Automatically sync dependencies and run your script
```

---

## RabbitMQ

Start a local broker with Docker:

```bash
docker run -d --name rabbit \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
# management UI → http://localhost:15672  (guest/guest)
```

### Consumer — handler registry

Add handlers in `rmq/consumer.py` or any module imported at startup:

```python
from rmq.consumer import register_handler


@register_handler("order.placed")
async def handle_order(data: dict) -> None:
    ...
```

Message body must be JSON with `{"event_type": "...", ...}`.
Unknown event types are dropped (nack, no requeue).
Handler exceptions cause a nack with requeue=True (one retry).

### Publisher

```python
from rmq.publisher import publish

await publish("user.created", {"id": user_id, "email": email})
```

Uses a lazy robust connection — reconnects automatically.

### Health checker

Probes every `RMQ_HEALTH_INTERVAL_SECS` seconds.
Hook into state transitions in `rmq/health.py`:

```python
async def on_rabbitmq_down(state):
    await notify_slack(f"RabbitMQ DOWN at {state.last_dead}")


async def on_rabbitmq_recovered(state):
    await notify_slack(f"RabbitMQ back at {state.last_alive}")
```

Poll current state:

```bash
curl http://localhost:8000/health/rabbitmq
```

---

## Background jobs

```bash
# async job (asyncio.gather fan-out)
curl -X POST http://localhost:8000/jobs/ \
  -H "Content-Type: application/json" \
  -d '{"payload": {"key": "value"}, "sync": false}'

# sync/blocking job (runs in thread pool via to_thread)
curl -X POST http://localhost:8000/jobs/ \
  -d '{"payload": {"key": "value"}, "sync": true}'

# poll status
curl http://localhost:8000/jobs/<job_id>
```

---

## Tests

```bash
uv run pytest -v
```

RabbitMQ is mocked in all tests — no broker needed.

---

## Config reference (`.env`)

| Variable                   | Default                              | Description                       |
|----------------------------|--------------------------------------|-----------------------------------|
| `RABBITMQ_URL`             | `amqp://guest:guest@localhost:5672/` | broker connection string          |
| `RMQ_QUEUE`                | `task_queue`                         | consumer queue name               |
| `RMQ_EXCHANGE`             | `app_exchange`                       | topic exchange name               |
| `RMQ_ROUTING_KEY`          | `#`                                  | binding key (# = all topics)      |
| `RMQ_DLX`                  | `dlx`                                | dead-letter exchange name         |
| `RMQ_PREFETCH_COUNT`       | `10`                                 | max unacked messages per consumer |
| `RMQ_HEALTH_INTERVAL_SECS` | `10`                                 | health probe interval (seconds)   |
| `RMQ_PROBE_TIMEOUT_SECS`   | `3`                                  | TCP connect timeout for probe     |

Sample App Starting Logs:

```bash
(fastapi_project) fastapi_project|master⚡ ⇒ uv run uvicorn main:app --reload
INFO:     Will watch for changes in these directories: ['/home/krishna/Desktop/My_Space/fastapi_project']
INFO:     Uvicorn running on http://127.0.0.1:9000 (Press CTRL+C to quit)
INFO:     Started reloader process [291365] using WatchFiles
INFO:     Started server process [291367]
INFO:     Waiting for application startup.
2026-07-10 13:21:35,733 [INFO] main — 🚀 Starting up
2026-07-10 13:21:35,865 [INFO] rmq.consumer — 📨 RabbitMQ consumer starting
2026-07-10 13:21:35,865 [INFO] rmq.health — 🩺 RabbitMQ health checker started  interval=10.0s  probe_timeout=3.0s
INFO:     Application startup complete.
2026-07-10 13:21:35,872 [INFO] rmq.consumer — ✅ RabbitMQ consumer connected
2026-07-10 13:21:35,873 [INFO] rmq.health — 🟢 RabbitMQ RECOVERED at 2026-07-10T07:51:35.873565  (was dead since: None)
2026-07-10 13:21:35,879 [INFO] rmq.consumer — 📨 Consuming from queue='task_queue'  exchange='app_exchange'  routing_key='#'

^CINFO:     Shutting down
INFO:     Waiting for application shutdown.
2026-07-10 13:21:50,905 [INFO] main — 🛑 Shutting down
2026-07-10 13:21:50,907 [INFO] rmq.consumer — 📨 RabbitMQ consumer cancelled — shutting down
2026-07-10 13:21:50,907 [INFO] rmq.publisher — 📤 Publisher connection closed
INFO:     Application shutdown complete.
INFO:     Finished server process [291367]
INFO:     Stopping reloader process [291365]
```

# Migrating the B changes

Use Alembic — it compares your ORM models against the actual DB and generates migration scripts automatically.
Setup:

```bash
uv add alembic
uv run alembic init alembic   # creates alembic/ folder + alembic.ini
```

Tell Alembic about your models and DB — edit alembic/env.py:

```python
from core.config import settings
from models.orm import Base  # imports all mapped classes

config.set_main_option("sqlalchemy.url", settings.DB_URL)

target_metadata = Base.metadata  # ← Alembic diffs against this
```
Workflow:
```bash
# 1. You change a model (add a column, new table, etc.)
# 2. Auto-generate migration — Alembic diffs ORM vs DB

uv run  alembic revision --autogenerate -m "add phone column to users"


# 3. Review the generated file in alembic/versions/

# 4. Apply it

uv run alembic upgrade head

# Rollback one step
uv run alembic downgrade -1

# See current state

uv run alembic current

```


One gotcha with async — Alembic runs sync by default. Edit alembic/env.py to use async:

```python
from sqlalchemy.ext.asyncio import async_engine_from_config
import asyncio

def run_migrations_online():
    connectable = async_engine_from_config(config.get_section(config.config_ini_section))

    async def do_migrations():
        async with connectable.connect() as connection:
            await connection.run_sync(context.run_migrations)

    asyncio.run(do_migrations())
```

Never auto-apply in production — always review generated scripts first. Alembic's --autogenerate misses things like
index changes on existing columns and custom constraints — always sanity check before upgrade head.

## Running the alembic commands

After Fixing the issue with Sync and Async, the flow will be smooth

```bash
(fastapi_project) fastapi_project|master⚡ ⇒ uv run alembic revision --autogenerate -m "initial"
INFO  [alembic.runtime.migration] Context impl MySQLImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
INFO  [alembic.runtime.plugins] setting up autogenerate plugin alembic.autogenerate.schemas
INFO  [alembic.runtime.plugins] setting up autogenerate plugin alembic.autogenerate.tables
INFO  [alembic.runtime.plugins] setting up autogenerate plugin alembic.autogenerate.types
INFO  [alembic.runtime.plugins] setting up autogenerate plugin alembic.autogenerate.constraints
INFO  [alembic.runtime.plugins] setting up autogenerate plugin alembic.autogenerate.defaults
INFO  [alembic.runtime.plugins] setting up autogenerate plugin alembic.autogenerate.comments
INFO  [alembic.autogenerate.compare.tables] Detected added table 'users'
INFO  [alembic.autogenerate.compare.constraints] Detected added index 'ix_users_email' on '('email',)'
INFO  [alembic.autogenerate.compare.tables] Detected added table 'jobs'
INFO  [alembic.autogenerate.compare.constraints] Detected added index 'ix_jobs_user_id' on '('user_id',)'
  Generating /home/xxxxx/xxxxxxxx/fastapi_project/alembic/versions/89cc93b7e46d_initial.py ...  done
```

```bash
(fastapi_project) fastapi_project|master⚡ ⇒ uuv run alembic upgrade head
INFO  [alembic.runtime.migration] Context impl MySQLImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
INFO  [alembic.runtime.migration] Running upgrade  -> 89cc93b7e46d, initial
```
```bash
(fastapi_project) fastapi_project|master⚡ ⇒ uuv run alembic current

INFO  [alembic.runtime.migration] Context impl MySQLImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
89cc93b7e46d (head)
```
