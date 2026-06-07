import asyncio
import json
import time
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import StreamingResponse

from app.transport.client_registry import register, unregister, stats
from app.utils import logger
from config import get_settings

router = APIRouter(tags=["events"])
VALID_STATUSES = {"pending", "shipped", "delivered"}


@router.get("/events/stream")
async def sse_stream(request: Request):
    settings = get_settings()
    queue: asyncio.Queue[dict | None] = asyncio.Queue(maxsize=500)

    async def send(payload: dict) -> None:
        await queue.put(payload)

    client_id = await register(send, "sse")

    async def event_generator():
        yield _sse_event("connected", {"clientId": client_id, "timestamp": datetime.now(timezone.utc).isoformat()})
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=settings.sse_heartbeat_interval_s)
                    yield _sse_event("order:change", payload)
                    queue.task_done()
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            await unregister(client_id)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


def _sse_event(event_name: str, data: dict) -> str:
    return f"event: {event_name}\ndata: {json.dumps(data, default=str)}\n\n"


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    filter_status: str | None = None
    queue: asyncio.Queue[dict | None] = asyncio.Queue(maxsize=500)

    async def send(payload: dict) -> None:
        data = payload.get("data") or {}
        prev = payload.get("previous") or {}
        if filter_status and data.get("status") != filter_status and prev.get("status") != filter_status:
            return
        await queue.put(payload)

    client_id = await register(send, "websocket")

    await ws.send_json({
        "type": "connected", "clientId": client_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hint": 'Send {"type":"filter","status":"pending|shipped|delivered"} to filter.',
    })

    async def receive_task():
        nonlocal filter_status
        try:
            while True:
                raw = await ws.receive_text()
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "filter":
                        new_status = msg.get("status")
                        if new_status in VALID_STATUSES or new_status is None:
                            filter_status = new_status
                            await ws.send_json({"type": "filter:ack", "status": filter_status})
                    elif msg.get("type") == "ping":
                        await ws.send_json({"type": "pong", "ts": int(time.time() * 1000)})
                except json.JSONDecodeError:
                    pass
        except (WebSocketDisconnect, Exception):
            pass

    async def send_task():
        try:
            while True:
                payload = await queue.get()
                await ws.send_json({"type": "order:change", "payload": payload}, mode="text")
                queue.task_done()
        except (WebSocketDisconnect, Exception):
            pass

    recv_t = asyncio.create_task(receive_task())
    send_t = asyncio.create_task(send_task())
    done, pending = await asyncio.wait([recv_t, send_t], return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()

    await unregister(client_id)


@router.get("/events/stats")
async def events_stats():
    return {"success": True, **stats()}
