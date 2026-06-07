# realtime-db-updates

Push every database change to connected clients the moment it commits. No polling. No triggers. No missed events.

PostgreSQL writes to its WAL on every commit. This service tails that log through a logical replication slot, decodes the binary pgoutput frames, and fans out to every connected client over SSE or WebSocket — typically under 5 ms from commit to delivery.

```
INSERT / UPDATE / DELETE
        │
        ▼  WAL commit
PostgreSQL logical replication slot
        │
        ▼  psycopg2 daemon thread · pgoutput binary decoder
asyncio.Queue
        │
        ▼  broadcaster drain loop
client_registry.broadcast()
        │
        ├──► /events/stream   (SSE — browser, curl)
        └──► /ws              (WebSocket — CLI, apps)
```

---

## Why WAL replication and not LISTEN/NOTIFY

`NOTIFY` payloads cap at 8 KB, truncate silently, and don't carry the old row on UPDATE. WAL replication has no size limit, delivers the full before/after row on every change, and acknowledges by LSN — so a server restart replays from exactly where it left off rather than dropping events.

---

## Stack

| | |
|---|---|
| **Language** | Python 3.12 |
| **Web framework** | FastAPI 0.111 + Uvicorn |
| **Database** | PostgreSQL 16 (wal_level=logical) |
| **Query pool** | asyncpg |
| **CDC / replication** | psycopg2 LogicalReplicationConnection |
| **WAL decoder** | Custom pgoutput binary parser (no external deps) |
| **Config** | Pydantic Settings |
| **Container** | Docker + Docker Compose |

The WAL decoder is written from scratch against the [pgoutput wire protocol](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html). It handles Relation, Insert, Update, and Delete frames, caches relation metadata by OID, and acks each LSN after delivery so Postgres can advance WAL cleanup.

psycopg2 runs the replication cursor in a daemon thread — the only synchronous part of the system. Events cross into the asyncio world via `run_coroutine_threadsafe` onto a bounded `asyncio.Queue(maxsize=10_000)`.

---

## Project layout

```
├── server/
│   ├── app/
│   │   ├── db/
│   │   │   ├── replication.py   # WAL CDC engine
│   │   │   └── pool.py          # asyncpg query pool
│   │   ├── transport/
│   │   │   └── client_registry.py  # SSE + WS fan-out
│   │   ├── events/
│   │   │   └── broadcaster.py   # queue → clients
│   │   ├── routes/
│   │   │   ├── orders.py        # REST CRUD
│   │   │   └── events.py        # SSE stream + WebSocket
│   │   ├── utils/logger.py
│   │   └── main.py              # FastAPI app + lifespan
│   ├── config/settings.py       # Pydantic Settings
│   └── run.py
├── client/watch.py              # terminal SSE client
├── scripts/
│   ├── init.sql                 # schema + publication + seed
│   ├── seed.py                  # populate via REST
│   └── stress.py                # concurrent load test
└── tests/                       # 57 tests, 0 external services needed
```

---

## Running it

**Docker — one command:**

```bash
git clone <repo>
cd realtime-db-updates
docker compose up --build
```

Open `http://localhost:3000`. The dashboard shows live INSERT / UPDATE / DELETE events as they happen.

**Local development:**

```bash
docker compose up postgres -d

cd server
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp ../.env.example .env
python run.py
```

**Terminal client:**

```bash
python client/watch.py               # watch all changes
python client/watch.py --filter shipped   # filter by status
python client/watch.py --demo        # auto-advance statuses every 3 s
```

**Seed and stress test:**

```bash
python scripts/seed.py --count 25
python scripts/stress.py --workers 5 --cycles 20
```

---

## API

```
GET    /api/orders          list all orders
GET    /api/orders/{id}     get one
POST   /api/orders          create
PATCH  /api/orders/{id}     update status
DELETE /api/orders/{id}     delete

GET    /events/stream       SSE stream (text/event-stream)
GET    /events/stats        connected client count
WS     /ws                  WebSocket endpoint
GET    /health
GET    /docs                Swagger UI
```

SSE event shape:

```
event: order:change
data: {"operation":"UPDATE","timestamp":"...","data":{...},"previous":{...}}
```

WebSocket clients can filter by status after connecting:

```json
{ "type": "filter", "status": "shipped" }
```

---

## Tests

57 tests, no database or running server required:

```bash
cd server
pip install -r requirements-test.txt
cd ..
pytest tests/ -v
```

| File | What it covers |
|---|---|
| `test_replication.py` | pgoutput binary decoder — hand-crafted WAL frames |
| `test_client_registry.py` | fan-out, dead client pruning, concurrent safety |
| `test_orders_api.py` | full REST CRUD with mocked asyncpg pool |
| `test_broadcaster.py` | asyncio drain loop, error recovery |
| `test_sse.py` | SSE connected event, stats endpoint |

---

## Scaling out

Currently uses an in-memory dict for connected clients. To run multiple workers or pods, swap the registry for Redis pub/sub:

```python
# each worker subscribes and forwards to its local clients
async for message in pubsub.listen():
    payload = json.loads(message["data"])
    await broadcast(payload)
```

`client_registry.py`, SSE, and WebSocket handlers need no changes — only the broadcaster changes.
