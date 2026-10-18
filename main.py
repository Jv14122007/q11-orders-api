
import time
import uuid
import base64
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

# CORS middleware wraps the application, so its headers are applied
# to normal responses and rate-limit responses.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

lock = Lock()

# Fixed catalog containing IDs 1 through 59.
catalog = [
    {"id": i, "item": f"Order {i}", "status": "created"}
    for i in range(1, TOTAL_ORDERS + 1)
]

# In-memory stores.
idempotency_store = {}
client_requests = defaultdict(deque)


class OrderRequest(BaseModel):
    item: Optional[str] = "New order"


@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    # Do not count CORS preflight requests against the limit.
    if request.method == "OPTIONS":
        return await call_next(request)

    # Only rate-limit requests that supply a client ID.
    client_id = request.headers.get("X-Client-Id")
    if not client_id:
        return await call_next(request)

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


@app.post("/orders", status_code=201)
def create_order(
    body: OrderRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    with lock:
        if idempotency_key in idempotency_store:
            return JSONResponse(
                status_code=200,
                content=idempotency_store[idempotency_key],
            )

        order = {
            "id": str(uuid.uuid4()),
            "item": body.item,
            "status": "created",
        }
        idempotency_store[idempotency_key] = order

        return JSONResponse(status_code=201, content=order)


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1, le=59),
    cursor: Optional[str] = Query(default=None),
):
    # Cursors encode the last returned order ID.
    if cursor is None or cursor == "":
        start_id = 1
    else:
        try:
            decoded = base64.urlsafe_b64decode(
                cursor.encode("ascii") + b"=" * (-len(cursor) % 4)
            ).decode("ascii")
            start_id = int(decoded) + 1
        except Exception:
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

    if items and items[-1]["id"] < TOTAL_ORDERS:
        next_cursor = base64.urlsafe_b64encode(
            str(items[-1]["id"]).encode("ascii")
        ).decode("ascii").rstrip("=")
    else:
        next_cursor = None

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
