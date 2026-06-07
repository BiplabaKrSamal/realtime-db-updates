from datetime import datetime
from typing import Literal

import asyncpg
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.db.pool import get_pool
from app.utils import logger

router = APIRouter(prefix="/api/orders", tags=["orders"])
StatusType = Literal["pending", "shipped", "delivered"]


class OrderCreate(BaseModel):
    customer_name: str
    product_name: str
    status: StatusType = "pending"


class OrderUpdate(BaseModel):
    status: StatusType


class OrderRow(BaseModel):
    id: int
    customer_name: str
    product_name: str
    status: str
    updated_at: datetime
    model_config = {"from_attributes": True}


async def pool_dep() -> asyncpg.Pool:
    return await get_pool()


@router.get("/", response_model=list[OrderRow])
async def list_orders(pool: asyncpg.Pool = Depends(pool_dep)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM orders ORDER BY updated_at DESC")
    return [dict(r) for r in rows]


@router.get("/{order_id}", response_model=OrderRow)
async def get_order(order_id: int, pool: asyncpg.Pool = Depends(pool_dep)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")
    return dict(row)


@router.post("/", response_model=OrderRow, status_code=201)
async def create_order(body: OrderCreate, pool: asyncpg.Pool = Depends(pool_dep)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO orders (customer_name, product_name, status, updated_at) VALUES ($1, $2, $3, NOW()) RETURNING *",
            body.customer_name, body.product_name, body.status,
        )
    logger.info("Order created", extra={"id": row["id"]})
    return dict(row)


@router.patch("/{order_id}", response_model=OrderRow)
async def update_order(order_id: int, body: OrderUpdate, pool: asyncpg.Pool = Depends(pool_dep)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *",
            body.status, order_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")
    logger.info("Order updated", extra={"id": order_id, "status": body.status})
    return dict(row)


@router.delete("/{order_id}", response_model=OrderRow)
async def delete_order(order_id: int, pool: asyncpg.Pool = Depends(pool_dep)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("DELETE FROM orders WHERE id = $1 RETURNING *", order_id)
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")
    logger.info("Order deleted", extra={"id": order_id})
    return dict(row)
