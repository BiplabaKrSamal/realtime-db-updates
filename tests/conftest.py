import asyncio
import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_USER", "aptuser")
os.environ.setdefault("POSTGRES_PASSWORD", "aptpassword")
os.environ.setdefault("POSTGRES_DB", "aptdb_test")


def make_order_record(id=1, customer_name="Alice", product_name="Headphones", status="pending"):
    from datetime import datetime, timezone
    return {
        "id": id,
        "customer_name": customer_name,
        "product_name": product_name,
        "status": status,
        "updated_at": datetime.now(timezone.utc),
    }


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def mock_pool():
    pool = AsyncMock()
    conn = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.fetch.return_value = []
    conn.fetchrow.return_value = None
    conn.execute.return_value = "OK"
    return pool, conn


@pytest_asyncio.fixture
async def app(mock_pool) -> FastAPI:
    pool, _ = mock_pool
    with (
        patch("app.db.pool.get_pool", AsyncMock(return_value=pool)),
        patch("app.db.pool.close_pool", AsyncMock()),
        patch("app.db.replication.start_replication", AsyncMock()),
        patch("app.db.replication.stop_replication", AsyncMock()),
        patch("app.events.broadcaster.start_broadcasting", MagicMock()),
        patch("app.events.broadcaster.stop_broadcasting", AsyncMock()),
    ):
        from app.main import create_app
        from config.settings import get_settings
        get_settings.cache_clear()
        _app = create_app()
        yield _app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
