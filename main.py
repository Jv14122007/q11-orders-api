
import time
import uuid
from collections import defaultdict, deque
from threading import Lock
from typing import Optional

from fastapi import FastAPI, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI()

TOTAL_ORDERS = 59
RATE_LIMIT = 17
WINDOW_SECONDS = 10

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

lock = Lock()

# Fixed catalog: IDs 1 through 59.
catalog = [
    {"id": i, "item": f"Order {i}", "status": "created"}
    for i in range(1, TOTAL_ORDERS + 1)
]

# In-memory idempotency store.
idempotency_store = {}

# Per-client sliding-window rate-limit timestamps.
client_requests = defaultdict(deque)


@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    # Apply rate limiting to API requests using each client's own bucket.
    client_id = request.headers.get("X-Client-Id", "anonymous")
    now = time.monotonic()

    with lock:
        timestamps = client_requests[client_id]

        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1,
                int(WINDOW_SECONDS - (now - timestamps[0]) + 0.999),
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        timestamps.append(now)

    return await call_next(request)


class OrderRequest(BaseModel):
    item: Optional[str] = "New order"


@app.post("/orders", status_code=201)
def create_order(
    body: OrderRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    with lock:
        if idempotency_key in idempotency_store:
            return idempotency_store[idempotency_key]

        order = {
            "id": str(uuid.uuid4()),
            "item": body.item,
            "status": "created",
        }
        idempotency_store[idempotency_key] = order
        return order


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1, le=59),
    cursor: Optional[str] = Query(default=None),
):
    try:
        start_id = int(cursor) + 1 if cursor is not None else 1
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"detail": "Invalid cursor"},
        )

    if start_id < 1 or start_id > TOTAL_ORDERS + 1:
        return JSONResponse(
            status_code=400,
            content={"detail": "Invalid cursor"},
        )

    items = catalog[start_id - 1 : start_id - 1 + limit]
    last_id = items[-1]["id"] if items else start_id - 1

    next_cursor = (
        str(last_id)
        if last_id < TOTAL_ORDERS
        else None
    )

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
