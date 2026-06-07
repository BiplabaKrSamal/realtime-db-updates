import asyncio
import struct
import threading
import time
from datetime import datetime, timezone
from typing import Any

from config import get_settings
from app.utils import logger

change_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=10_000)
_replication_task: asyncio.Task | None = None
_stop_event = threading.Event()
_relations: dict[int, dict] = {}


def _read_cstring(data: bytes, pos: int) -> tuple[str, int]:
    end = data.index(b"\x00", pos)
    return data[pos:end].decode("utf-8"), end + 1


def _decode_text_datum(data: bytes, pos: int) -> tuple[Any, int]:
    kind = chr(data[pos]); pos += 1
    if kind == "n":
        return None, pos
    if kind == "u":
        return "__unchanged__", pos
    length = struct.unpack_from(">I", data, pos)[0]; pos += 4
    value  = data[pos: pos + length].decode("utf-8");  pos += length
    return value, pos


def _decode_tuple(data: bytes, pos: int, columns: list[str]) -> tuple[dict, int]:
    n_cols = struct.unpack_from(">H", data, pos)[0]; pos += 2
    row: dict[str, Any] = {}
    for i in range(n_cols):
        val, pos = _decode_text_datum(data, pos)
        if i < len(columns):
            row[columns[i]] = val
    return row, pos


def _decode_message(data: bytes) -> dict | None:
    if not data:
        return None
    msg_type = chr(data[0])

    if msg_type == "R":
        pos    = 1
        rel_id = struct.unpack_from(">I", data, pos)[0]; pos += 4
        _,  pos = _read_cstring(data, pos)
        _,  pos = _read_cstring(data, pos)
        pos += 1
        n_cols  = struct.unpack_from(">H", data, pos)[0]; pos += 2
        columns: list[str] = []
        for _ in range(n_cols):
            pos += 1
            col_name, pos = _read_cstring(data, pos)
            pos += 4; pos += 4
            columns.append(col_name)
        _relations[rel_id] = {"columns": columns}
        return None

    if msg_type == "I":
        pos    = 1
        rel_id = struct.unpack_from(">I", data, pos)[0]; pos += 4
        pos   += 1
        cols   = _relations.get(rel_id, {}).get("columns", [])
        new_row, _ = _decode_tuple(data, pos, cols)
        return {"tag": "insert", "relation_id": rel_id, "new": new_row, "old": None}

    if msg_type == "U":
        pos    = 1
        rel_id = struct.unpack_from(">I", data, pos)[0]; pos += 4
        cols   = _relations.get(rel_id, {}).get("columns", [])
        old_row = None
        marker  = chr(data[pos]); pos += 1
        if marker in ("O", "K"):
            old_row, pos = _decode_tuple(data, pos, cols)
            pos += 1
        new_row, _ = _decode_tuple(data, pos, cols)
        return {"tag": "update", "relation_id": rel_id, "new": new_row, "old": old_row}

    if msg_type == "D":
        pos    = 1
        rel_id = struct.unpack_from(">I", data, pos)[0]; pos += 4
        cols   = _relations.get(rel_id, {}).get("columns", [])
        pos   += 1
        old_row, _ = _decode_tuple(data, pos, cols)
        return {"tag": "delete", "relation_id": rel_id, "new": None, "old": old_row}

    return None


def _build_payload(operation: str, new_row: dict | None, old_row: dict | None = None) -> dict:
    return {
        "operation": operation,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data":      new_row if new_row is not None else old_row,
        "previous":  old_row,
    }


def _make_message_consumer(loop: asyncio.AbstractEventLoop):
    def consumer(msg) -> None:
        raw = bytes(msg.payload)
        try:
            decoded = _decode_message(raw)
            if decoded is None:
                msg.cursor.send_feedback(flush_lsn=msg.data_start)
                return

            tag = decoded["tag"]
            if tag == "insert":
                payload = _build_payload("INSERT", decoded["new"])
                logger.info("CDC INSERT", extra={"id": decoded["new"].get("id")})
            elif tag == "update":
                payload = _build_payload("UPDATE", decoded["new"], decoded["old"])
                logger.info("CDC UPDATE", extra={"id": decoded["new"].get("id")})
            elif tag == "delete":
                payload = _build_payload("DELETE", None, decoded["old"])
                logger.info("CDC DELETE", extra={"id": (decoded["old"] or {}).get("id")})
            else:
                msg.cursor.send_feedback(flush_lsn=msg.data_start)
                return

            asyncio.run_coroutine_threadsafe(change_queue.put(payload), loop)
            msg.cursor.send_feedback(flush_lsn=msg.data_start)

        except Exception as exc:
            logger.error("WAL decode error", extra={"error": str(exc)})
            try:
                msg.cursor.send_feedback(flush_lsn=msg.data_start)
            except Exception:
                pass

    return consumer


def _sync_replication_loop(loop: asyncio.AbstractEventLoop) -> None:
    import psycopg2
    import psycopg2.extras
    import psycopg2.errors

    settings = get_settings()
    slot     = settings.replication_slot_name
    pub      = settings.publication_name
    delay    = settings.replication_reconnect_delay_s
    consumer = _make_message_consumer(loop)

    while not _stop_event.is_set():
        conn = None
        try:
            logger.info("Replication connecting", extra={"slot": slot})
            conn = psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                dbname=settings.postgres_db,
                connection_factory=psycopg2.extras.LogicalReplicationConnection,
            )
            cur = conn.cursor()
            try:
                cur.create_replication_slot(slot, output_plugin="pgoutput")
            except psycopg2.errors.DuplicateObject:
                pass

            cur.start_replication(
                slot_name=slot,
                decode=False,
                options={"proto_version": "1", "publication_names": pub},
            )
            logger.info("Replication stream active")

            while not _stop_event.is_set():
                cur.consume_stream(consumer, keepalive_interval=10)

        except Exception as exc:
            if _stop_event.is_set():
                break
            logger.error("Replication error, reconnecting", extra={"error": str(exc)})
            time.sleep(delay)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    logger.info("Replication thread exiting")


async def start_replication() -> None:
    global _replication_task
    _stop_event.clear()
    loop = asyncio.get_running_loop()

    thread = threading.Thread(
        target=_sync_replication_loop,
        args=(loop,),
        daemon=True,
        name="replication-thread",
    )
    thread.start()
    logger.info("Replication thread started")

    async def _watcher():
        nonlocal thread
        try:
            while True:
                await asyncio.sleep(5)
                if not thread.is_alive() and not _stop_event.is_set():
                    logger.error("Replication thread died, restarting")
                    thread = threading.Thread(
                        target=_sync_replication_loop,
                        args=(loop,),
                        daemon=True,
                        name="replication-thread",
                    )
                    thread.start()
        except asyncio.CancelledError:
            pass

    _replication_task = asyncio.create_task(_watcher(), name="replication-watcher")


async def stop_replication() -> None:
    global _replication_task
    _stop_event.set()
    if _replication_task and not _replication_task.done():
        _replication_task.cancel()
        try:
            await _replication_task
        except asyncio.CancelledError:
            pass
    logger.info("Replication stopped")
