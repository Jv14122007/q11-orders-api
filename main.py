
import asyncio
import base64
import json
import math
import os
import time
import uuid
from collections import defaultdict, deque
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="Orders API", version="1.0.0")

# Allow browser-based graders to call the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

TOTAL_ORDERS = 59
RATE_LIMIT = 17
WINDOW_SECONDS = 10

# Fixed catalog for pagination: IDs 1 through 59.
CATALOG = [
    {"id": i, "item": f"Item {i}", "status": "available"}
    for i in range(1, TOTAL_ORDERS + 1)
]

# In-memory storage. Suitable for a single Render instance.
idempotency_store: dict[str, dict[str, Any]] = {}
rate_limit_store = defaultdict(deque)
rate_limit_lock = asyncio.Lock()


@app.middleware("http")
async def enforce_rate_limit(request: Request, call_next):
    # Do not count browser CORS preflight requests.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "default")
    now = time.monotonic()

    async with rate_limit_lock:
        timestamps = rate_limit_store[client_id]

        # Remove requests outside the rolling 10-second window.
        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1, math.ceil(WINDOW_SECONDS - (now - timestamps[0]))
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        timestamps.append(now)

    return await call_next(request)


@app.get("/")
async def root():
    return {
        "message": "Orders API is running",
        "total_orders": TOTAL_ORDERS,
        "rate_limit": RATE_LIMIT,
        "window_seconds": WINDOW_SECONDS,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    key = idempotency_key.strip()
    if not key:
        raise HTTPException(
            status_code=400, detail="Idempotency-Key cannot be empty"
        )

    # Return the original order for a repeated key.
    if key in idempotency_store:
        return JSONResponse(
            status_code=200,
            content=idempotency_store[key],
        )

    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        body = {}

    if not isinstance(body, dict):
        body = {"data": body}

    order = {
        "id": str(uuid.uuid4()),
        **body,
    }

    # Store the result so this key cannot create a duplicate order.
    idempotency_store[key] = order

    return JSONResponse(status_code=201, content=order)


def encode_cursor(offset: int) -> str:
    raw = str(offset).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> int:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        offset = int(base64.urlsafe_b64decode(padded).decode("utf-8"))
        if offset < 0 or offset > TOTAL_ORDERS:
            raise ValueError
        return offset
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


@app.get("/orders")
async def list_orders(
    limit: int = Query(default=10, ge=1),
    cursor: str | None = Query(default=None),
):
    offset = decode_cursor(cursor) if cursor else 0

    # Never return more items than the requested limit.
    items = CATALOG[offset : offset + limit]
    next_offset = offset + len(items)

    next_cursor = (
        encode_cursor(next_offset)
        if next_offset < TOTAL_ORDERS
        else None
    )

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
