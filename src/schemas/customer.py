from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field, EmailStr

class CustomerTier(str, Enum):
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"
    PLATINUM = "PLATINUM"

class CustomerSchema(BaseModel):
    customer_id: str = Field(..., min_length=3, description="Deterministic primary key e.g. CUST-0001")
    customer_name: str = Field(..., min_length=1)
    customer_email: str = Field(..., min_length=3)
    customer_tier: CustomerTier = Field(default=CustomerTier.BRONZE)
    country_code: str = Field(..., min_length=2, max_length=3)
    created_at: str = Field(..., description="ISO-8601 timestamp")

CUSTOMER_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR NOT NULL,
    customer_email VARCHAR NOT NULL,
    customer_tier VARCHAR NOT NULL,
    country_code VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);
"""
