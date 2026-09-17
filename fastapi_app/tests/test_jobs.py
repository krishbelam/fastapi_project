"""
tests/test_jobs.py
──────────────────
Tests for background job submission and status polling.
"""

import asyncio
import pytest
from httpx import ASGITransport, AsyncClient

from main import app


@pytest.fixture
async def client():
    async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_submit_async_job(client):
    resp = await client.post("/jobs/", json={"payload": {"key": "value"}, "sync": False})
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "pending"
    assert "job_id" in data


@pytest.mark.asyncio
async def test_submit_sync_job(client):
    resp = await client.post("/jobs/", json={"payload": {"x": 1}, "sync": True})
    assert resp.status_code == 202
    assert resp.json()["status"] == "pending"


@pytest.mark.asyncio
async def test_job_status_polling(client):
    # Submit
    resp = await client.post("/jobs/", json={"payload": {"n": 42}, "sync": False})
    job_id = resp.json()["job_id"]

    # Poll until done (async job takes ~1s)
    for _ in range(20):
        await asyncio.sleep(0.2)
        status_resp = await client.get(f"/jobs/{job_id}")
        if status_resp.json()["status"] == "done":
            break

    assert status_resp.json()["status"] == "done"
    assert status_resp.json()["result"] is not None


@pytest.mark.asyncio
async def test_job_not_found(client):
    resp = await client.get("/jobs/does-not-exist")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_jobs(client):
    await client.post("/jobs/", json={"payload": {}, "sync": False})
    resp = await client.get("/jobs/")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
