"""
Scalable, reproducible synthetic dataset generator with deterministic seeding.
Generates baseline, incremental (CDC), out-of-order, and deliberate edge-case batches.
"""
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple
import pyarrow as pa
import pyarrow.parquet as pq

from config.settings import RAW_DATA_DIR, RANDOM_SEED
from src.schemas.customer import CustomerSchema, CustomerTier
from src.schemas.order import OrderStatus, PaymentMethod

class DatasetGenerator:
    """
    Deterministic dataset generator supporting:
    - Dimension tables (customers)
    - Baseline orders (initial snapshot)
    - Incremental delta orders (inserts, updates, soft-deletes, late arrivals, out-of-order updates)
    - Deliberately labeled edge cases (malformed JSON, negative amounts, missing keys, FK violations)
    - High-volume scalable data generation (up to 100k+ records)
    """

    def __init__(self, seed: int = RANDOM_SEED):
        self.seed = seed
        self.rng = random.Random(seed)
        self.countries = ["US", "IN", "DE", "GB", "CA", "FR", "JP", "AU"]
        self.customers: List[Dict[str, Any]] = []
        self.customer_ids: List[str] = []

    def generate_customers(self, count: int = 500, save_path: Path = None) -> List[Dict[str, Any]]:
        """Generates dimension customer records."""
        self.customers = []
        self.customer_ids = []
        base_time = datetime(2026, 1, 1, 10, 0, 0)

        for i in range(1, count + 1):
            cust_id = f"CUST-{i:05d}"
            self.customer_ids.append(cust_id)
            cust = {
                "customer_id": cust_id,
                "customer_name": f"Customer_{i}",
                "customer_email": f"user_{i}@example.com",
                "customer_tier": self.rng.choice(list(CustomerTier)).value,
                "country_code": self.rng.choice(self.countries),
                "created_at": (base_time + timedelta(hours=i)).isoformat() + "Z"
            }
            self.customers.append(cust)

        if save_path:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "w", encoding="utf-8") as f:
                for c in self.customers:
                    f.write(json.dumps(c) + "\n")

        return self.customers

    def generate_initial_orders(
        self,
        count: int = 1000,
        start_date: str = "2026-10-25",
        num_days: int = 3,
        save_path: Path = None
    ) -> List[Dict[str, Any]]:
        """
        Generates initial baseline orders across multiple partitions (dates).
        All orders in this initial batch are active (is_deleted=False).
        """
        if not self.customer_ids:
            self.generate_customers(count=max(200, count // 10))

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        orders: List[Dict[str, Any]] = []

        statuses = [OrderStatus.PENDING.value, OrderStatus.CONFIRMED.value, OrderStatus.SHIPPED.value]
        methods = [m.value for m in PaymentMethod]

        for i in range(1, count + 1):
            day_offset = self.rng.randint(0, num_days - 1)
            order_date_dt = start_dt + timedelta(days=day_offset)
            order_date_str = order_date_dt.strftime("%Y-%m-%d")

            second_offset = self.rng.randint(0, 86399)
            order_time = order_date_dt + timedelta(seconds=second_offset)
            time_str = order_time.strftime("%Y-%m-%dT%H:%M:%SZ")

            order_id = f"ORD-{order_date_dt.strftime('%Y%m%d')}-{i:06d}"
            amount = round(self.rng.uniform(10.0, 1500.0), 2)
            items_count = self.rng.randint(1, 12)

            order = {
                "order_id": order_id,
                "customer_id": self.rng.choice(self.customer_ids),
                "order_date": order_date_str,
                "order_timestamp": time_str,
                "order_status": self.rng.choice(statuses),
                "payment_method": self.rng.choice(methods),
                "currency": "USD",
                "amount": amount,
                "items_count": items_count,
                "updated_at": time_str,
                "is_deleted": False
            }
            orders.append(order)

        if save_path:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "w", encoding="utf-8") as f:
                for o in orders:
                    f.write(json.dumps(o) + "\n")

        return orders

    def generate_incremental_orders(
        self,
        existing_orders: List[Dict[str, Any]],
        new_inserts_count: int = 300,
        updates_ratio: float = 0.20,
        soft_deletes_ratio: float = 0.05,
        out_of_order_count: int = 15,
        late_arriving_count: int = 25,
        incremental_date: str = "2026-10-28",
        save_path: Path = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Generates an incremental CDC delta batch containing:
        1. New inserts for current date
        2. Status and amount updates to existing orders
        3. Soft deletes (is_deleted = True)
        4. Late-arriving orders for older partition dates
        5. Out-of-order updates (older updated_at timestamp)
        """
        inc_dt = datetime.strptime(incremental_date, "%Y-%m-%d")
        delta_records: List[Dict[str, Any]] = []
        metrics = {
            "new_inserts": 0,
            "updates": 0,
            "soft_deletes": 0,
            "late_arriving": 0,
            "out_of_order": 0
        }

        # 1. Updates to existing orders
        num_updates = int(len(existing_orders) * updates_ratio)
        sampled_for_update = self.rng.sample(existing_orders, min(num_updates, len(existing_orders)))

        for orig in sampled_for_update:
            updated_order = orig.copy()
            # Advance updated_at to incremental day
            advance_seconds = self.rng.randint(3600, 43200)
            new_updated_at = (inc_dt + timedelta(seconds=advance_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Update status or amount
            if updated_order["order_status"] in [OrderStatus.PENDING.value, OrderStatus.CONFIRMED.value]:
                updated_order["order_status"] = OrderStatus.DELIVERED.value
            else:
                updated_order["order_status"] = OrderStatus.REFUNDED.value

            updated_order["amount"] = round(updated_order["amount"] + self.rng.uniform(5.0, 50.0), 2)
            updated_order["updated_at"] = new_updated_at
            delta_records.append(updated_order)
            metrics["updates"] += 1

        # 2. Soft deletes
        num_deletes = int(len(existing_orders) * soft_deletes_ratio)
        remaining_candidates = [o for o in existing_orders if o not in sampled_for_update]
        sampled_for_delete = self.rng.sample(remaining_candidates, min(num_deletes, len(remaining_candidates)))

        for orig in sampled_for_delete:
            del_order = orig.copy()
            del_time = (inc_dt + timedelta(seconds=self.rng.randint(100, 3600))).strftime("%Y-%m-%dT%H:%M:%SZ")
            del_order["order_status"] = OrderStatus.CANCELLED.value
            del_order["is_deleted"] = True
            del_order["updated_at"] = del_time
            delta_records.append(del_order)
            metrics["soft_deletes"] += 1

        # 3. New inserts for current date
        start_id = len(existing_orders) + 1000
        for i in range(new_inserts_count):
            order_time = (inc_dt + timedelta(seconds=self.rng.randint(0, 86399))).strftime("%Y-%m-%dT%H:%M:%SZ")
            order_id = f"ORD-{inc_dt.strftime('%Y%m%d')}-{start_id + i:06d}"
            amount = round(self.rng.uniform(15.0, 2000.0), 2)

            order = {
                "order_id": order_id,
                "customer_id": self.rng.choice(self.customer_ids),
                "order_date": incremental_date,
                "order_timestamp": order_time,
                "order_status": OrderStatus.PENDING.value,
                "payment_method": self.rng.choice([m.value for m in PaymentMethod]),
                "currency": "USD",
                "amount": amount,
                "items_count": self.rng.randint(1, 10),
                "updated_at": order_time,
                "is_deleted": False
            }
            delta_records.append(order)
            metrics["new_inserts"] += 1

        # 4. Late-arriving orders for historical date (e.g. 2026-10-25)
        for i in range(late_arriving_count):
            hist_date = "2026-10-25"
            order_time = "2026-10-25T14:00:00Z"
            # Arrived now on incremental date
            current_ingest_time = (inc_dt + timedelta(seconds=self.rng.randint(100, 1800))).strftime("%Y-%m-%dT%H:%M:%SZ")
            order_id = f"ORD-20261025-LATE-{i:04d}"

            order = {
                "order_id": order_id,
                "customer_id": self.rng.choice(self.customer_ids),
                "order_date": hist_date,
                "order_timestamp": order_time,
                "order_status": OrderStatus.CONFIRMED.value,
                "payment_method": OrderStatus.PENDING.value,
                "currency": "USD",
                "amount": round(self.rng.uniform(20.0, 500.0), 2),
                "items_count": 2,
                "updated_at": current_ingest_time,
                "is_deleted": False
            }
            delta_records.append(order)
            metrics["late_arriving"] += 1

        # 5. Out-of-order updates (older updated_at timestamp than existing record)
        # These represent delayed network packets or CDC replay that should NOT overwrite newer records
        if sampled_for_update:
            for i in range(min(out_of_order_count, len(sampled_for_update))):
                stale_order = sampled_for_update[i].copy()
                # Stale timestamp: 2 days earlier than order creation!
                stale_time = "2026-10-20T00:00:00Z"
                stale_order["updated_at"] = stale_time
                stale_order["amount"] = 9999.99  # bogus stale value that must NOT overwrite
                stale_order["order_status"] = OrderStatus.PENDING.value
                delta_records.append(stale_order)
                metrics["out_of_order"] += 1

        # Shuffle delta records to simulate realistic un-ordered event stream
        self.rng.shuffle(delta_records)

        if save_path:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "w", encoding="utf-8") as f:
                for d in delta_records:
                    f.write(json.dumps(d) + "\n")

        return delta_records, metrics

    def generate_edge_case_batch(self, save_path: Path = None) -> List[Dict[str, Any]]:
        """
        Generates deliberate edge cases and malformed records to exercise quarantine logic:
        1. Malformed JSON / unparseable record
        2. Missing required field (missing order_id, missing customer_id)
        3. Negative amount (business rule violation)
        4. Zero items count (business rule violation)
        5. Invalid foreign key (customer_id does not exist in dimension table)
        6. Intra-batch duplicate records with different timestamps
        7. Unexpected schema change / extra fields
        """
        edge_cases: List[Dict[str, Any]] = [
            # 1. Missing required field: order_id
            {
                "customer_id": self.customer_ids[0] if self.customer_ids else "CUST-00001",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T12:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 150.00,
                "items_count": 2,
                "updated_at": "2026-10-28T12:00:00Z",
                "is_deleted": False,
                "_test_label": "MISSING_PRIMARY_KEY"
            },
            # 2. Missing required field: customer_id
            {
                "order_id": "ORD-EDGE-0002",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T12:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 150.00,
                "items_count": 2,
                "updated_at": "2026-10-28T12:00:00Z",
                "is_deleted": False,
                "_test_label": "MISSING_CUSTOMER_ID"
            },
            # 3. Negative amount
            {
                "order_id": "ORD-EDGE-0003",
                "customer_id": self.customer_ids[0] if self.customer_ids else "CUST-00001",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T12:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": -75.50,
                "items_count": 1,
                "updated_at": "2026-10-28T12:00:00Z",
                "is_deleted": False,
                "_test_label": "NEGATIVE_AMOUNT"
            },
            # 4. Zero items count
            {
                "order_id": "ORD-EDGE-0004",
                "customer_id": self.customer_ids[0] if self.customer_ids else "CUST-00001",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T12:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 99.00,
                "items_count": 0,
                "updated_at": "2026-10-28T12:00:00Z",
                "is_deleted": False,
                "_test_label": "ZERO_ITEMS_COUNT"
            },
            # 5. Invalid Foreign Key (Non-existent customer)
            {
                "order_id": "ORD-EDGE-0005",
                "customer_id": "CUST-NON-EXISTENT-99999",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T12:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 250.00,
                "items_count": 3,
                "updated_at": "2026-10-28T12:00:00Z",
                "is_deleted": False,
                "_test_label": "FOREIGN_KEY_VIOLATION"
            },
            # 6. Intra-batch duplicate 1 (Earlier timestamp)
            {
                "order_id": "ORD-EDGE-0006-DUP",
                "customer_id": self.customer_ids[0] if self.customer_ids else "CUST-00001",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T10:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 100.00,
                "items_count": 1,
                "updated_at": "2026-10-28T10:00:00Z",
                "is_deleted": False,
                "_test_label": "INTRA_BATCH_DUPLICATE_EARLIER"
            },
            # 6b. Intra-batch duplicate 2 (Later timestamp - should win)
            {
                "order_id": "ORD-EDGE-0006-DUP",
                "customer_id": self.customer_ids[0] if self.customer_ids else "CUST-00001",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T10:00:00Z",
                "order_status": "CONFIRMED",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 120.00,
                "items_count": 1,
                "updated_at": "2026-10-28T11:00:00Z",
                "is_deleted": False,
                "_test_label": "INTRA_BATCH_DUPLICATE_LATER"
            },
            # 7. Valid record with unexpected extra fields (Schema drift tolerance test)
            {
                "order_id": "ORD-EDGE-0007-EXTRA",
                "customer_id": self.customer_ids[0] if self.customer_ids else "CUST-00001",
                "order_date": "2026-10-28",
                "order_timestamp": "2026-10-28T12:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 310.00,
                "items_count": 4,
                "updated_at": "2026-10-28T12:00:00Z",
                "is_deleted": False,
                "unexpected_loyalty_points": 450,
                "unexpected_device_ip": "192.168.1.1",
                "_test_label": "UNEXPECTED_EXTRA_FIELDS"
            }
        ]

        if save_path:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "w", encoding="utf-8") as f:
                for ec in edge_cases:
                    f.write(json.dumps(ec) + "\n")
                # Also inject a completely malformed unparseable raw line
                f.write("{MALFORMED_NON_JSON_CORRUPTED_BYTES\n")

        return edge_cases

    def generate_large_scale_dataset(
        self,
        count: int = 100000,
        output_parquet_path: Path = None
    ) -> Path:
        """
        High-performance generator producing 100k+ records formatted as Apache Parquet.
        Uses PyArrow for high throughput.
        """
        if not self.customer_ids:
            self.generate_customers(count=1000)

        num_cust = len(self.customer_ids)
        start_date = datetime(2026, 10, 1)

        order_ids = [f"ORD-{i:08d}" for i in range(1, count + 1)]
        cust_choices = [self.customer_ids[self.rng.randint(0, num_cust - 1)] for _ in range(count)]
        dates = [(start_date + timedelta(days=self.rng.randint(0, 30))).strftime("%Y-%m-%d") for _ in range(count)]
        timestamps = [f"{dates[i]}T{self.rng.randint(0, 23):02d}:{self.rng.randint(0, 59):02d}:{self.rng.randint(0, 59):02d}Z" for i in range(count)]
        statuses = [self.rng.choice(list(OrderStatus)).value for _ in range(count)]
        methods = [self.rng.choice(list(PaymentMethod)).value for _ in range(count)]
        currencies = ["USD"] * count
        amounts = [round(self.rng.uniform(10.0, 2000.0), 2) for _ in range(count)]
        items = [self.rng.randint(1, 15) for _ in range(count)]
        is_deleted = [False] * count

        table = pa.Table.from_arrays([
            pa.array(order_ids, pa.string()),
            pa.array(cust_choices, pa.string()),
            pa.array(dates, pa.string()),
            pa.array(timestamps, pa.string()),
            pa.array(statuses, pa.string()),
            pa.array(methods, pa.string()),
            pa.array(currencies, pa.string()),
            pa.array(amounts, pa.float64()),
            pa.array(items, pa.int32()),
            pa.array(timestamps, pa.string()),
            pa.array(is_deleted, pa.bool_())
        ], names=[
            "order_id", "customer_id", "order_date", "order_timestamp", "order_status",
            "payment_method", "currency", "amount", "items_count", "updated_at", "is_deleted"
        ])

        if output_parquet_path:
            output_parquet_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, output_parquet_path, compression="snappy")

        return output_parquet_path
