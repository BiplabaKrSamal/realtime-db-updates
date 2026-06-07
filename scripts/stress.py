#!/usr/bin/env python3
import argparse
import asyncio
import json
import time
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--url",     default="http://localhost:3000")
parser.add_argument("--workers", type=int, default=3)
parser.add_argument("--cycles",  type=int, default=10)
args = parser.parse_args()

BASE_URL = args.url.rstrip("/")
WORKERS  = args.workers
CYCLES   = args.cycles


def api(method: str, path: str, body: dict | None = None) -> dict | None:
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(
        f"{BASE_URL}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"} if body else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  [{method} {path}]: {e}")
        return None


async def worker(worker_id: int):
    loop = asyncio.get_event_loop()
    for cycle in range(CYCLES):
        created = await loop.run_in_executor(None, api, "POST", "/api/orders", {
            "customer_name": f"StressUser-{worker_id}",
            "product_name":  f"Item-{cycle}",
            "status":        "pending",
        })
        if not created:
            continue
        oid = created["id"]
        await loop.run_in_executor(None, api, "PATCH",  f"/api/orders/{oid}", {"status": "shipped"})
        await loop.run_in_executor(None, api, "PATCH",  f"/api/orders/{oid}", {"status": "delivered"})
        await loop.run_in_executor(None, api, "DELETE", f"/api/orders/{oid}")
        print(f"  Worker {worker_id}: cycle {cycle + 1}/{CYCLES} ✓")


async def main():
    total = WORKERS * CYCLES * 4
    print(f"\nStress test: {WORKERS} workers × {CYCLES} cycles = {total} events\n")
    t0 = time.time()
    await asyncio.gather(*[worker(i + 1) for i in range(WORKERS)])
    elapsed = time.time() - t0
    print(f"\n✓ Done in {elapsed:.2f}s  ({total / elapsed:.0f} events/s)")


if __name__ == "__main__":
    asyncio.run(main())
