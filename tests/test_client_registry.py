import asyncio
import sys
import pathlib
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "server"))


@pytest.fixture(autouse=True)
def fresh_registry():
    import app.transport.client_registry as reg
    reg._clients.clear()
    yield reg
    reg._clients.clear()


@pytest.mark.asyncio
async def test_register_returns_uuid(fresh_registry):
    cid = await fresh_registry.register(AsyncMock(), "sse")
    assert isinstance(cid, str)
    assert len(cid) == 36


@pytest.mark.asyncio
async def test_register_increments_count(fresh_registry):
    assert fresh_registry.stats()["total"] == 0
    await fresh_registry.register(AsyncMock(), "sse")
    await fresh_registry.register(AsyncMock(), "websocket")
    assert fresh_registry.stats()["total"] == 2


@pytest.mark.asyncio
async def test_unregister_removes_client(fresh_registry):
    cid = await fresh_registry.register(AsyncMock(), "sse")
    await fresh_registry.unregister(cid)
    assert fresh_registry.stats()["total"] == 0


@pytest.mark.asyncio
async def test_unregister_unknown_id_is_noop(fresh_registry):
    await fresh_registry.unregister("does-not-exist")


@pytest.mark.asyncio
async def test_broadcast_reaches_all_clients(fresh_registry):
    s1, s2, s3 = AsyncMock(), AsyncMock(), AsyncMock()
    await fresh_registry.register(s1, "sse")
    await fresh_registry.register(s2, "websocket")
    await fresh_registry.register(s3, "sse")

    payload = {"operation": "INSERT", "data": {"id": "1"}}
    await fresh_registry.broadcast(payload)

    s1.assert_awaited_once_with(payload)
    s2.assert_awaited_once_with(payload)
    s3.assert_awaited_once_with(payload)


@pytest.mark.asyncio
async def test_broadcast_empty_registry_is_noop(fresh_registry):
    await fresh_registry.broadcast({"operation": "INSERT", "data": {}})


@pytest.mark.asyncio
async def test_broadcast_prunes_dead_clients(fresh_registry):
    good   = AsyncMock()
    broken = AsyncMock(side_effect=RuntimeError("connection reset"))

    await fresh_registry.register(good, "sse")
    await fresh_registry.register(broken, "websocket")

    await fresh_registry.broadcast({"operation": "UPDATE", "data": {"id": "2"}})

    good.assert_awaited_once()
    assert fresh_registry.stats()["total"] == 1


@pytest.mark.asyncio
async def test_broadcast_payload_is_passed_unchanged(fresh_registry):
    sink = AsyncMock()
    await fresh_registry.register(sink, "sse")
    payload = {"operation": "DELETE", "data": {"id": "99"}, "previous": {"status": "shipped"}}
    await fresh_registry.broadcast(payload)
    sink.assert_awaited_once_with(payload)


@pytest.mark.asyncio
async def test_concurrent_broadcasts_dont_race(fresh_registry):
    received = []

    async def sink(payload):
        received.append(payload)

    await fresh_registry.register(sink, "sse")
    await asyncio.gather(*[fresh_registry.broadcast({"operation": "INSERT", "n": i}) for i in range(50)])
    assert len(received) == 50


@pytest.mark.asyncio
async def test_stats_by_transport(fresh_registry):
    await fresh_registry.register(AsyncMock(), "sse")
    await fresh_registry.register(AsyncMock(), "sse")
    await fresh_registry.register(AsyncMock(), "websocket")
    s = fresh_registry.stats()
    assert s["total"] == 3
    assert s["by_transport"]["sse"] == 2
    assert s["by_transport"]["websocket"] == 1


@pytest.mark.asyncio
async def test_stats_empty(fresh_registry):
    s = fresh_registry.stats()
    assert s["total"] == 0
    assert s["by_transport"] == {}
