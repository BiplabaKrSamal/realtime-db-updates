import asyncio
import uuid
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

from app.utils import logger

_clients: dict[str, dict] = {}
_lock = asyncio.Lock()


async def register(send_fn: Callable[[dict], Coroutine[Any, Any, None]], transport: str) -> str:
    client_id = str(uuid.uuid4())
    async with _lock:
        _clients[client_id] = {"send": send_fn, "transport": transport, "connected_at": datetime.now(timezone.utc)}
    logger.info("Client connected", extra={"client_id": client_id[:8], "transport": transport, "total": len(_clients)})
    return client_id


async def unregister(client_id: str) -> None:
    async with _lock:
        client = _clients.pop(client_id, None)
    if client:
        logger.info("Client disconnected", extra={"client_id": client_id[:8], "transport": client["transport"], "total": len(_clients)})


async def broadcast(payload: dict) -> None:
    if not _clients:
        return
    async with _lock:
        snapshot = list(_clients.items())
    dead: list[str] = []
    await asyncio.gather(*[_safe_send(cid, c["send"], payload, dead) for cid, c in snapshot], return_exceptions=True)
    if dead:
        async with _lock:
            for cid in dead:
                _clients.pop(cid, None)


async def _safe_send(client_id: str, send_fn, payload: dict, dead: list) -> None:
    try:
        await send_fn(payload)
    except Exception as exc:
        logger.warning("Dead client removed", extra={"client_id": client_id[:8], "error": str(exc)})
        dead.append(client_id)


def stats() -> dict:
    by_transport: dict[str, int] = {}
    for c in _clients.values():
        t = c["transport"]
        by_transport[t] = by_transport.get(t, 0) + 1
    return {"total": len(_clients), "by_transport": by_transport}
