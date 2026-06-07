import asyncio
import sys
import pathlib
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "server"))


@pytest.fixture(autouse=True)
def reset_broadcaster():
    import app.events.broadcaster as bc
    bc._broadcaster_task = None
    yield
    bc._broadcaster_task = None


def _clear_queue():
    from app.db.replication import change_queue
    while not change_queue.empty():
        change_queue.get_nowait()
    return change_queue


@pytest.mark.asyncio
async def test_drain_loop_forwards_payload():
    received = []

    async def mock_broadcast(payload):
        received.append(payload)

    import app.events.broadcaster as bc
    q = _clear_queue()
    payload = {"operation": "INSERT", "data": {"id": "1"}, "previous": None, "timestamp": "t"}

    with patch("app.events.broadcaster.broadcast", side_effect=mock_broadcast):
        bc.start_broadcasting()
        await q.put(payload)
        await asyncio.sleep(0.05)
        await bc.stop_broadcasting()

    assert len(received) == 1
    assert received[0]["operation"] == "INSERT"


@pytest.mark.asyncio
async def test_drain_loop_handles_multiple_payloads():
    received = []

    async def mock_broadcast(payload):
        received.append(payload["operation"])

    import app.events.broadcaster as bc
    q = _clear_queue()

    with patch("app.events.broadcaster.broadcast", side_effect=mock_broadcast):
        bc.start_broadcasting()
        for op in ["INSERT", "UPDATE", "DELETE"]:
            await q.put({"operation": op, "data": {}, "previous": None, "timestamp": "t"})
        await asyncio.sleep(0.1)
        await bc.stop_broadcasting()

    assert received == ["INSERT", "UPDATE", "DELETE"]


@pytest.mark.asyncio
async def test_drain_loop_survives_broadcast_error():
    call_count = 0

    async def flaky_broadcast(payload):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient error")

    import app.events.broadcaster as bc
    q = _clear_queue()

    with patch("app.events.broadcaster.broadcast", side_effect=flaky_broadcast):
        bc.start_broadcasting()
        await q.put({"operation": "INSERT", "data": {}, "previous": None, "timestamp": "t"})
        await q.put({"operation": "UPDATE", "data": {}, "previous": None, "timestamp": "t"})
        await asyncio.sleep(0.1)
        await bc.stop_broadcasting()

    assert call_count == 2


@pytest.mark.asyncio
async def test_stop_broadcasting_is_idempotent():
    import app.events.broadcaster as bc
    bc.start_broadcasting()
    await bc.stop_broadcasting()
    await bc.stop_broadcasting()


@pytest.mark.asyncio
async def test_stop_broadcasting_when_never_started():
    import app.events.broadcaster as bc
    bc._broadcaster_task = None
    await bc.stop_broadcasting()
