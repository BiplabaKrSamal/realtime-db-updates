#!/usr/bin/env python3
import argparse
import asyncio
import json
import threading
import urllib.request
from datetime import datetime

R      = "\x1b[0m"
BOLD   = "\x1b[1m"
DIM    = "\x1b[2m"
GREEN  = "\x1b[32m"
YELLOW = "\x1b[33m"
BLUE   = "\x1b[34m"
CYAN   = "\x1b[36m"
RED    = "\x1b[31m"
MAGENTA = "\x1b[35m"
WHITE  = "\x1b[37m"

OP_COLOR     = {"INSERT": GREEN, "UPDATE": BLUE, "DELETE": RED}
STATUS_COLOR = {"pending": YELLOW, "shipped": BLUE, "delivered": GREEN}

parser = argparse.ArgumentParser()
parser.add_argument("--url",    default="http://localhost:3000")
parser.add_argument("--filter", default=None, choices=["pending", "shipped", "delivered"])
parser.add_argument("--demo",   action="store_true")
args = parser.parse_args()

BASE_URL = args.url.rstrip("/")
FILTER   = args.filter
DEMO     = args.demo


def pad(s, n):
    return str(s or "").ljust(n)[:n]


def print_header():
    import os
    os.system("clear" if os.name != "nt" else "cls")
    print(f"{BOLD}{CYAN}┌─────────────────────────────────────────────────────────────┐{R}")
    print(f"{BOLD}{CYAN}│  📦  Orders Real-Time Feed                                   │{R}")
    print(f"{BOLD}{CYAN}│  {DIM}Server : {pad(BASE_URL, 46)}{CYAN}│{R}")
    print(f"{BOLD}{CYAN}│  {DIM}Filter : {pad(FILTER or 'none', 46)}{CYAN}│{R}")
    print(f"{BOLD}{CYAN}│  {DIM}Demo   : {pad('ON' if DEMO else 'OFF', 46)}{CYAN}│{R}")
    print(f"{BOLD}{CYAN}└─────────────────────────────────────────────────────────────┘{R}")
    print()
    print(f"  {DIM}{'TIME':<10} {'OP':<8} {'ID':<5} {'CUSTOMER':<20} {'PRODUCT':<22} STATUS{R}")
    print(f"  {DIM}{'─' * 80}{R}")


def print_change(payload: dict):
    op   = payload.get("operation", "?")
    data = payload.get("data") or {}
    prev = payload.get("previous") or {}
    row  = data if data else prev

    if FILTER and row.get("status") != FILTER and prev.get("status") != FILTER:
        return

    prev_note = ""
    if op == "UPDATE" and prev.get("status") != data.get("status"):
        prev_note = f"  {DIM}(was: {prev.get('status', '?')}){R}"

    print(
        f"  {DIM}{datetime.now().strftime('%H:%M:%S')}{R}  "
        f"{OP_COLOR.get(op, WHITE)}{BOLD}{pad(op, 7)}{R}  "
        f"{WHITE}{pad(row.get('id', ''), 4)}{R}  "
        f"{pad(row.get('customer_name', ''), 19)}  "
        f"{pad(row.get('product_name', ''), 21)}  "
        f"{STATUS_COLOR.get(row.get('status', ''), WHITE)}{pad(row.get('status', ''), 11)}{R}"
        f"{prev_note}"
    )


def sse_subscribe(event_queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
    import http.client
    from urllib.parse import urlparse

    parsed = urlparse(f"{BASE_URL}/events/stream")
    host   = parsed.hostname
    port   = parsed.port or (443 if parsed.scheme == "https" else 80)

    while True:
        try:
            conn = http.client.HTTPConnection(host, port, timeout=60)
            conn.request("GET", parsed.path or "/events/stream", headers={"Accept": "text/event-stream"})
            resp = conn.getresponse()

            if resp.status != 200:
                print(f"\n  {RED}HTTP {resp.status} — is the server running?{R}")
                import time; time.sleep(3)
                continue

            buf = ""
            while True:
                chunk = resp.read(1).decode("utf-8", errors="ignore")
                if not chunk:
                    break
                buf += chunk
                if buf.endswith("\n\n"):
                    _process_block(buf.strip(), event_queue, loop)
                    buf = ""
        except Exception as e:
            print(f"\n  {RED}Connection error: {e}{R}")
            import time; time.sleep(3)


def _process_block(block: str, event_queue: asyncio.Queue, loop):
    event_name, data_line = "message", ""
    for line in block.splitlines():
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_line = line[5:].strip()
    if not data_line or block.startswith(":"):
        return
    try:
        parsed = json.loads(data_line)
        asyncio.run_coroutine_threadsafe(event_queue.put({"name": event_name, "data": parsed}), loop)
    except json.JSONDecodeError:
        pass


def _api(method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(
        f"{BASE_URL}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"} if body else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


STATUSES = ["pending", "shipped", "delivered"]


async def demo_loop():
    await asyncio.sleep(1.5)
    print(f"\n  {MAGENTA}Demo mode active — advancing statuses every 3 s{R}\n")
    while True:
        import random
        result = _api("GET", "/api/orders")
        if result:
            orders = result if isinstance(result, list) else []
            if orders:
                order = random.choice(orders)
                idx = STATUSES.index(order["status"]) if order["status"] in STATUSES else 0
                _api("PATCH", f"/api/orders/{order['id']}", {"status": STATUSES[(idx + 1) % len(STATUSES)]})
        await asyncio.sleep(3)


async def main():
    print_header()
    print(f"\n  {DIM}Connecting to {BASE_URL}/events/stream…{R}\n")

    event_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    threading.Thread(target=sse_subscribe, args=(event_queue, loop), daemon=True).start()

    if DEMO:
        asyncio.create_task(demo_loop())

    while True:
        event = await event_queue.get()
        if event["name"] == "connected":
            print(f"  {GREEN}✓ Connected{R}  {DIM}(id: {event['data'].get('clientId', '')[:8]}){R}\n")
        elif event["name"] == "order:change":
            print_change(event["data"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n\n  {DIM}Bye!{R}\n")
