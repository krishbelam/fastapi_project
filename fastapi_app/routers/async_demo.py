"""
routers/async_demo.py
─────────────────────
Live runnable demos for each asyncio primitive.
"""

import asyncio
import time

from fastapi import APIRouter

router = APIRouter(prefix="/async-demo", tags=["Asyncio Patterns"])


@router.get("/gather")
async def demo_gather():
    """
    asyncio.gather — run N coroutines concurrently.
    Total time ≈ max(individual times), not sum.
    """

    async def task(n: int) -> str:
        await asyncio.sleep(0.2 * n)
        return f"task-{n}-done"

    results = await asyncio.gather(task(1), task(2), task(3))
    return {"results": results, "note": "all 3 ran concurrently"}


@router.get("/gather-exception")
async def demo_gather_exception():
    """
    asyncio.gather with return_exceptions=True.
    Failed coroutines return the exception instead of propagating.
    """

    async def good() -> str:
        await asyncio.sleep(0.1)
        return "ok"

    async def bad() -> str:
        await asyncio.sleep(0.05)
        raise ValueError("something went wrong")

    results = await asyncio.gather(good(), bad(), return_exceptions=True)
    return {
        "results": [
            str(r) if isinstance(r, Exception) else r
            for r in results
        ]
    }


@router.get("/create-task")
async def demo_create_task():
    """
    asyncio.create_task — schedule a coroutine now, do other work, await later.
    Unlike gather, you get a handle to cancel or check status individually.
    """

    async def background_work() -> str:
        await asyncio.sleep(0.1)
        return "background result"

    task = asyncio.create_task(background_work())
    # ↑ already running concurrently while we do other work here
    await asyncio.sleep(0.05)  # simulate other work in parallel
    result = await task
    return {"result": result}


@router.get("/to-thread")
async def demo_to_thread():
    """
    asyncio.to_thread — off-load blocking code to a thread pool.
    The event loop stays free to serve other requests while this runs.
    """

    def slow_blocking() -> str:
        time.sleep(0.3)  # blocks in thread pool, NOT the event loop
        return "blocking work done in thread"

    result = await asyncio.to_thread(slow_blocking)
    return {"result": result}


@router.get("/sleep")
async def demo_sleep():
    """
    asyncio.sleep — yields control back to the event loop.
    Other coroutines/requests run while this waits.
    Contrast with time.sleep() which would freeze the whole process.
    """
    await asyncio.sleep(0.1)
    return {"slept": "0.1s — event loop was free the whole time"}
