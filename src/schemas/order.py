from enum import Enum
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from config.settings import TAX_RATE

class OrderStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"

class PaymentMethod(str, Enum):
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    UPI = "UPI"
    NET_BANKING = "NET_BANKING"
    CRYPTO = "CRYPTO"

class OrderRawSchema(BaseModel):
    order_id: str = Field(..., min_length=1, description="Deterministic primary key e.g. ORD-20261025-00001")
    customer_id: str = Field(..., min_length=1, description="Foreign key to dim_customers")
    order_date: str = Field(..., description="Partition key in format YYYY-MM-DD")
    order_timestamp: str = Field(..., description="ISO-8601 order creation timestamp")
    order_status: OrderStatus = Field(...)
    payment_method: PaymentMethod = Field(...)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    amount: float = Field(..., description="Order subtotal strictly greater than 0")
    items_count: int = Field(..., ge=1, description="Must have at least 1 item")
    updated_at: str = Field(..., description="ISO-8601 event timestamp for CDC / watermark")
    is_deleted: bool = Field(default=False, description="Soft-delete indicator")

    @field_validator("amount")
    @classmethod
    def validate_amount_positive(cls, v: float) -> float:
        if v <= 0.0:
            raise ValueError(f"Amount must be strictly positive (> 0.0), got {v}")
        return round(v, 2)

    @field_validator("order_date")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format for order_date, expected YYYY-MM-DD, got {v}")
        return v

    @field_validator("order_timestamp", "updated_at")
    @classmethod
    def validate_iso_timestamps(cls, v: str) -> str:
        try:
            # Supports both with/without timezone or Z
            clean_v = v.replace("Z", "+00:00")
            datetime.fromisoformat(clean_v)
        except Exception:
            raise ValueError(f"Invalid ISO-8601 timestamp: {v}")
        return v

ORDER_COLUMNS = [
    "order_id", "customer_id", "order_date", "order_timestamp",
    "order_status", "payment_method", "currency", "amount",
    "tax_amount", "total_amount", "items_count", "updated_at",
    "is_deleted", "run_id", "source_file", "ingested_at"
]

class OrderProcessedSchema(OrderRawSchema):
    tax_amount: float = Field(..., ge=0.0)
    total_amount: float = Field(..., ge=0.0)
    run_id: str = Field(..., min_length=1)
    source_file: str = Field(..., min_length=1)
    ingested_at: str = Field(..., description="Pipeline ingestion timestamp")

    @classmethod
    def from_raw(cls, raw: OrderRawSchema, run_id: str, source_file: str, ingested_at: str) -> "OrderProcessedSchema":
        tax = round(raw.amount * TAX_RATE, 2)
        total = round(raw.amount + tax, 2)
        raw_dict = raw.model_dump()
        raw_dict.update({
            "tax_amount": tax,
            "total_amount": total,
            "run_id": run_id,
            "source_file": source_file,
            "ingested_at": ingested_at
        })
        return cls(**raw_dict)

    def to_ordered_dict(self) -> dict:
        d = self.model_dump()
        d["order_date"] = str(d["order_date"])
        # Return dict with exact keys in ORDER_COLUMNS order
        return {col: d[col] for col in ORDER_COLUMNS}

def get_orders_table_ddl(table_name: str) -> str:
    """Generates standard DDL for the orders analytical tables."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        order_id VARCHAR PRIMARY KEY,
        customer_id VARCHAR NOT NULL,
        order_date DATE NOT NULL,
        order_timestamp TIMESTAMP NOT NULL,
        order_status VARCHAR NOT NULL,
        payment_method VARCHAR NOT NULL,
        currency VARCHAR NOT NULL,
        amount DOUBLE NOT NULL,
        tax_amount DOUBLE NOT NULL,
        total_amount DOUBLE NOT NULL,
        items_count INTEGER NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        is_deleted BOOLEAN NOT NULL,
        run_id VARCHAR NOT NULL,
        source_file VARCHAR NOT NULL,
        ingested_at TIMESTAMP NOT NULL
    );
    """
