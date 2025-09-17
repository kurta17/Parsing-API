from typing import List, Literal, Optional

from pydantic import BaseModel, Field, conint, confloat


class Contact(BaseModel):
    name: str = Field(..., example="Jane Doe")
    email: str = Field(..., example="jane.doe@example.com")
    phone: str = Field(..., example="+1-202-555-0136")
    country: str = Field(..., example="United States")


class LineItem(BaseModel):
    sku: str = Field(..., example="SKU-ABCD-12345")
    name: str = Field(..., example="Compute vCPU-hours")
    qty: conint(ge=1) = Field(..., example=12)
    unit_price: confloat(ge=0) = Field(..., example=0.041)
    amount: confloat(ge=0) = Field(..., example=0.492)
    usage_month: str = Field(..., example="2025-08-01")


class Order(BaseModel):
    order_id: int = Field(..., example=42)
    account_id: int = Field(..., example=98765)
    company: str = Field(..., example="Acme Corp")
    contact: Contact
    status: Literal["created", "confirmed", "invoiced", "paid"] = "created"
    currency: Literal["USD", "EUR", "GBP"] = "USD"
    lines: List[LineItem]
    subtotal: confloat(ge=0) = Field(..., example=123.45)
    tax: confloat(ge=0) = Field(..., example=8.64)
    total: confloat(ge=0) = Field(..., example=132.09)
    created_at: str = Field(..., example="2025-09-10T12:34:56Z")
    source: str = Field(..., example="mock")
