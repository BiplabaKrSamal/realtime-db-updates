#!/usr/bin/env python3
import argparse
import json
import random
import urllib.request
import urllib.error

parser = argparse.ArgumentParser()
parser.add_argument("--url",   default="http://localhost:3000")
parser.add_argument("--count", type=int, default=10)
args = parser.parse_args()

BASE_URL = args.url.rstrip("/")
COUNT    = args.count

CUSTOMERS = [
    "Alice Johnson", "Bob Smith", "Carol Williams", "David Brown",
    "Eva Martinez", "Frank Lee", "Grace Kim", "Henry Patel",
    "Isla Chen", "Jack Wilson", "Karen Davis", "Liam Taylor",
]
PRODUCTS = [
    "Wireless Headphones", "Mechanical Keyboard", "USB-C Hub", "Standing Desk Mat",
    "4K Webcam", "Ergonomic Chair", "Dual Monitor Arm", "LED Desk Lamp",
    "NVMe SSD 1TB", "Noise-Cancelling Earbuds", "Stream Deck", "Thunderbolt Dock",
]
STATUSES = ["pending", "shipped", "delivered"]


def post(path: str, body: dict) -> dict | None:
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        f"{BASE_URL}{path}", data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"  ✗ HTTP {e.code}: {e.read().decode()}")
    except Exception as e:
        print(f"  ✗ {e}")
    return None


def main():
    print(f"Seeding {COUNT} orders to {BASE_URL}…\n")
    ok = 0
    for _ in range(COUNT):
        result = post("/api/orders", {
            "customer_name": random.choice(CUSTOMERS),
            "product_name":  random.choice(PRODUCTS),
            "status":        random.choice(STATUSES),
        })
        if result:
            print(f"  ✓ #{result['id']} — {result['customer_name']} / {result['product_name']} [{result['status']}]")
            ok += 1
    print(f"\nDone: {ok}/{COUNT} orders created.")


if __name__ == "__main__":
    main()
