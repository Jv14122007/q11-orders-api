
import asyncio
import base64
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

TOTAL_ORDERS = 59
RATE_LIMIT = 17
WINDOW_SECONDS = 10

# Fixed catalog with IDs 1 through 59.
CATALOG = [
    {"id": i, "item": f"Item {i}", "status": "available"}
    for i in range(1, TOTAL_ORDERS + 1)
]

# In-memory stores (use one Render instance during grading).
idempotency_store: dict[str, dict[str, Any]] = {}
idempotency_lock = asyncio.Lock()

rate_limit_store = defaultdict(deque)
rate_limit_lock = asyncio.Lock()


@app.middleware("http")
async def enforce_rate_limit(request: Request, call_next):
    # Browser preflight requests must not consume quota.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "default").strip()
    if not client_id:
        client_id = "default"

    now = time.monotonic()

    async with rate_limit_lock:
        timestamps = rate_limit_store[client_id]

        # Rolling 10-second window.
        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1,
                math.ceil(WINDOW_SECONDS - (now - timestamps[0])),
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


@app.get("/orders")
async def list_orders(
    limit: int = Query(default=10, ge=1),
    cursor: str | None = Query(default=None),
):
    if cursor:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            offset = int(
                base64.urlsafe_b64decode(padded).decode("utf-8")
            )
            if not 0 <= offset <= TOTAL_ORDERS:
                raise ValueError
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor")
    else:
        offset = 0

    items = CATALOG[offset:offset + limit]
    next_offset = offset + len(items)

    next_cursor = None
    if next_offset < TOTAL_ORDERS:
        next_cursor = (
            base64.urlsafe_b64encode(
                str(next_offset).encode("utf-8")
            )
            .decode("ascii")
            .rstrip("=")
        )

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    key = idempotency_key.strip()
    if not key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key cannot be empty",
        )

    async with idempotency_lock:
        # Same key returns the same order without creating a duplicate.
        if key in idempotency_store:
            return JSONResponse(
                status_code=200,
                content=idempotency_store[key],
            )

        try:
            body = await request.json()
        except Exception:
            body = {}

        if not isinstance(body, dict):
            body = {"data": body}

        order = {
            "id": str(uuid.uuid4()),
            **body,
        }

        idempotency_store[key] = order

        return JSONResponse(status_code=201, content=order)


# Wrap the entire application in CORS so even 429 responses
# include the required CORS headers.
app = CORSMiddleware(
    app=app,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
