import asyncio

from app.db.replication import change_queue
from app.transport.client_registry import broadcast
from app.utils import logger

_broadcaster_task: asyncio.Task | None = None


async def _drain_loop() -> None:
    while True:
        try:
            payload = await change_queue.get()
            await broadcast(payload)
            change_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Broadcaster error", extra={"error": str(exc)})


def start_broadcasting() -> None:
    global _broadcaster_task
    _broadcaster_task = asyncio.create_task(_drain_loop(), name="broadcaster")
    logger.info("Broadcaster started")


async def stop_broadcasting() -> None:
    global _broadcaster_task
    if _broadcaster_task and not _broadcaster_task.done():
        _broadcaster_task.cancel()
        try:
            await _broadcaster_task
        except asyncio.CancelledError:
            pass
    logger.info("Broadcaster stopped")
