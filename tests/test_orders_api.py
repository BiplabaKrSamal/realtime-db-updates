import sys
import pathlib
from datetime import datetime, timezone

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "server"))

SAMPLE_ORDER = {
    "id": 1,
    "customer_name": "Alice Johnson",
    "product_name": "Wireless Headphones",
    "status": "pending",
    "updated_at": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
}

SAMPLE_ORDER_2 = {
    "id": 2,
    "customer_name": "Bob Smith",
    "product_name": "Mechanical Keyboard",
    "status": "shipped",
    "updated_at": datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
}


@pytest.mark.asyncio
async def test_list_orders_empty(client, mock_pool):
    _, conn = mock_pool
    conn.fetch.return_value = []
    resp = await client.get("/api/orders")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_orders_returns_all(client, mock_pool):
    _, conn = mock_pool
    conn.fetch.return_value = [SAMPLE_ORDER, SAMPLE_ORDER_2]
    resp = await client.get("/api/orders")
    data = resp.json()
    assert resp.status_code == 200
    assert len(data) == 2
    assert data[0]["id"] == 1


@pytest.mark.asyncio
async def test_get_order_found(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = SAMPLE_ORDER
    resp = await client.get("/api/orders/1")
    assert resp.status_code == 200
    assert resp.json()["id"] == 1
    assert resp.json()["status"] == "pending"


@pytest.mark.asyncio
async def test_get_order_not_found(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = None
    resp = await client.get("/api/orders/999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_create_order_success(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = SAMPLE_ORDER
    resp = await client.post("/api/orders", json={
        "customer_name": "Alice Johnson",
        "product_name": "Wireless Headphones",
        "status": "pending",
    })
    assert resp.status_code == 201
    assert resp.json()["id"] == 1


@pytest.mark.asyncio
async def test_create_order_default_status_is_pending(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = SAMPLE_ORDER
    resp = await client.post("/api/orders", json={"customer_name": "Test", "product_name": "Widget"})
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_create_order_invalid_status(client, mock_pool):
    resp = await client.post("/api/orders", json={
        "customer_name": "Test", "product_name": "Widget", "status": "unknown_status",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_order_missing_required_fields(client, mock_pool):
    resp = await client.post("/api/orders", json={"status": "pending"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_update_order_status(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = {**SAMPLE_ORDER, "status": "shipped"}
    resp = await client.patch("/api/orders/1", json={"status": "shipped"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "shipped"


@pytest.mark.asyncio
async def test_update_order_not_found(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = None
    resp = await client.patch("/api/orders/999", json={"status": "shipped"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_order_invalid_status(client, mock_pool):
    resp = await client.patch("/api/orders/1", json={"status": "broken"})
    assert resp.status_code == 422


@pytest.mark.parametrize("valid_status", ["pending", "shipped", "delivered"])
@pytest.mark.asyncio
async def test_update_order_all_valid_statuses(valid_status, client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = {**SAMPLE_ORDER, "status": valid_status}
    resp = await client.patch("/api/orders/1", json={"status": valid_status})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_delete_order_success(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = SAMPLE_ORDER
    resp = await client.delete("/api/orders/1")
    assert resp.status_code == 200
    assert resp.json()["id"] == 1


@pytest.mark.asyncio
async def test_delete_order_not_found(client, mock_pool):
    _, conn = mock_pool
    conn.fetchrow.return_value = None
    resp = await client.delete("/api/orders/999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
