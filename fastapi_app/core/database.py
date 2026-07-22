# core/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from core import settings

# ── Engine ────────────────────────────────────────────────────────────────────
# One engine per process — holds the connection pool
engine = create_async_engine(
    settings.DB_URL,          # mysql+aiomysql://user:pass@host/db
    pool_size=settings.DB_POOL_SIZE,             # base connections kept alive
    max_overflow=settings.MAX_OVERFLOW_SIZE,          # extra connections allowed under burst
    pool_pre_ping=settings.POOL_PRE_PING,       # checks connection health before use (detects stale)
    # echo=settings.APP_DEBUG,  # logs SQL — True in dev, False in prod
    echo=False,
    pool_timeout=settings.DB_POOL_TIMEOUT, #  wait up to N seconds for a connection before raising
    pool_recycle=settings.DB_POOL_RECYCLE_TIME,  # recycle connections every 30min — prevents stale
)

# ── Session factory ───────────────────────────────────────────────────────────
# AsyncSessionLocal() creates a new session — called once per request
AsyncSessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,   # don't expire objects after commit (async-safe)
    class_=AsyncSession,
)

# ── Dependency ────────────────────────────────────────────────────────────────
# Injected into routes via Depends(get_db)
# Opens session → yields → commits → closes (even on exception)
# correct — rollback on exception, commit only on success
async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise