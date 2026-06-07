from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.db.pool import get_pool, close_pool
from app.db.replication import start_replication, stop_replication
from app.events.broadcaster import start_broadcasting, stop_broadcasting
from app.routes import orders_router, events_router
from app.utils import logger
from config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("Server starting", extra={"port": settings.port})

    await get_pool()
    start_broadcasting()
    await start_replication()

    logger.info("Server ready", extra={
        "rest":   f"http://{settings.host}:{settings.port}/api/orders",
        "sse":    f"http://{settings.host}:{settings.port}/events/stream",
        "ws":     f"ws://{settings.host}:{settings.port}/ws",
        "docs":   f"http://{settings.host}:{settings.port}/docs",
    })

    yield

    await stop_replication()
    await stop_broadcasting()
    await close_pool()
    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Apt Real-Time DB Updates",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    app.include_router(orders_router)
    app.include_router(events_router)

    @app.get("/health", tags=["system"])
    async def health():
        return {"status": "ok"}

    static_dir = Path(__file__).parent / "public"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app


app = create_app()
