import asyncio
import json
import sys
import pathlib
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "server"))


def _parse_sse_events(raw: str) -> list[dict]:
    events, current = [], {}
    for line in raw.splitlines():
        if line.startswith("event:"):
            current["event"] = line[6:].strip()
        elif line.startswith("data:"):
            current["data"] = json.loads(line[5:].strip())
        elif line.startswith(":"):
            current["comment"] = line[1:].strip()
        elif line == "" and current:
            events.append(current)
            current = {}
    if current:
        events.append(current)
    return events


@pytest.mark.asyncio
async def test_stats_endpoint_returns_counts(client):
    resp = await client.get("/events/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert "total" in body
    assert "by_transport" in body
    assert body["total"] == 0


@pytest.mark.asyncio
async def test_sse_sends_connected_event_then_closes(client):
    import app.transport.client_registry as reg
    reg._clients.clear()

    async with client.stream("GET", "/events/stream") as resp:
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers["content-type"]
        buffer = ""
        async for chunk in resp.aiter_text():
            buffer += chunk
            if "event: connected" in buffer and "\n\n" in buffer:
                break

    parsed = _parse_sse_events(buffer)
    assert len(parsed) >= 1
    assert parsed[0]["event"] == "connected"
    assert "clientId" in parsed[0]["data"]
    assert "timestamp" in parsed[0]["data"]
    reg._clients.clear()


@pytest.mark.asyncio
async def test_stats_counts_connected_sse_client(client):
    import app.transport.client_registry as reg
    reg._clients.clear()

    cid = await reg.register(AsyncMock(), "sse")
    resp = await client.get("/events/stats")
    body = resp.json()
    assert body["total"] == 1
    assert body["by_transport"].get("sse") == 1

    await reg.unregister(cid)
    reg._clients.clear()
