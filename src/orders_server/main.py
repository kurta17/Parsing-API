import asyncio
import random
from datetime import datetime, timezone
from typing import List, Literal, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from faker import Faker

from orders_server.models import Order, Contact, LineItem


# -----------------------------
# App & rate limiting
# -----------------------------
app = FastAPI(
    title="Rate-Limited Mock API (Orders)",
    version="1.1.0",
    description="""
A tiny demo API for classroom exercises on **threads / asyncio / httpx**.

- **Rate limited** with SlowAPI (`20 requests/second` per client IP)
- **Occasional 5xx** to exercise retry logic
- **Pydantic models** for clear OpenAPI/Swagger schemas
    """.strip(),
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rl_handler(request: Request, exc: RateLimitExceeded):
    # Return Retry-After so clients can be polite
    return JSONResponse(
        status_code=429,
        content={"detail": "rate limit exceeded"},
        headers={"Retry-After": "1"},
    )


# -----------------------------
# Fake data setup
# -----------------------------
PRODUCTS = [
    ("Cloud Storage - Standard Tier", 0.023),
    ("Cloud Storage - Coldline", 0.007),
    ("Compute vCPU-hours", 0.041),
    ("Managed DB - Small", 29.0),
    ("Managed DB - Medium", 99.0),
    ("Edge CDN GB", 0.08),
    ("Support Plan - Silver", 199.0),
    ("Support Plan - Gold", 799.0),
]
CURRENCIES = ["USD", "EUR", "GBP"]
STATUSES = ["created", "confirmed", "invoiced", "paid"]


def _now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_order_model(item_id: int) -> Order:
    f = Faker()
    f.seed_instance(item_id)  # deterministic per id

    account_id = f.pyint(min_value=10000, max_value=99999)
    company = f.company()
    contact = Contact(
        name=f.name(),
        email=f.company_email(),
        phone=f.phone_number(),
        country=f.country(),
    )

    n_lines = f.pyint(min_value=1, max_value=3)
    lines: List[LineItem] = []
    subtotal = 0.0
    for _ in range(n_lines):
        name, unit_price = random.choice(PRODUCTS)
        qty = f.pyint(min_value=1, max_value=50)
        amount = round(unit_price * qty, 2)
        subtotal += amount
        lines.append(
            LineItem(
                sku=f.bothify(text="SKU-????-#####"),
                name=name,
                qty=qty,
                unit_price=round(unit_price, 4),
                amount=amount,
                usage_month=f.date_this_year().isoformat(),
            )
        )

    currency = random.choice(CURRENCIES)
    tax_rate = 0.07 if currency == "USD" else 0.20
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)

    return Order(
        order_id=item_id,
        account_id=account_id,
        company=company,
        contact=contact,
        status=random.choice(STATUSES),
        currency=currency,
        lines=lines,
        subtotal=round(subtotal, 2),
        tax=tax,
        total=total,
        created_at=_now_z(),
        source="mock",
    )


# -----------------------------
# Routes
# -----------------------------
@app.get(
    "/",
    summary="API root",
    tags=["meta"],
)
async def root():
    return {"service": "mock-orders", "docs": "/docs", "health": "/healthz"}


@app.get(
    "/healthz",
    summary="Health check",
    tags=["meta"],
)
async def healthz():
    return {"status": "ok", "time": _now_z()}


@app.get(
    "/item/{item_id}",
    response_model=Order,
    summary="Get an order by ID",
    tags=["orders"],
    responses={
        429: {"description": "Rate limit exceeded"},
        500: {"description": "Injected flaky upstream error"},
    },
)
@limiter.limit("20/second")
async def get_item(request: Request, item_id: int):
    """Return a deterministic fake order for the given ID.

    Notes:
    - Random **5xx** occurs ~10% to simulate flaky upstreams (exercise client retries).
    - Rate limit: **20 req/sec** per client IP → returns **429** with `Retry-After: 1`.
    """
    # Simulated I/O latency
    await asyncio.sleep(random.uniform(0.05, 0.15))

    # Flakiness to exercise retry logic
    if random.random() < 0.10:
        raise HTTPException(status_code=500, detail="flaky upstream")

    return make_order_model(item_id)


# -----------------------------
# Run:
# uvicorn server:app --reload --port 8000
# Swagger UI: http://127.0.0.1:8000/docs
# ReDoc:      http://127.0.0.1:8000/redoc
# -----------------------------
