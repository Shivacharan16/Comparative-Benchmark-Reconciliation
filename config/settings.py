"""
Global configuration and directory paths for the Data Engineering pipeline.
"""
from pathlib import Path
import os

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"
TARGETS_DIR = DATA_DIR / "targets"
DATABASE_PATH = TARGETS_DIR / "analytics_warehouse.duckdb"

# Audit and reports
AUDIT_LOGS_DIR = DATA_DIR / "audit_logs"
REPORTS_DIR = BASE_DIR / "reports"

# Ensure runtime directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, QUARANTINE_DIR, TARGETS_DIR, AUDIT_LOGS_DIR, REPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Pipeline Constants
RANDOM_SEED = 42
DEFAULT_START_DATE = "2026-10-25"
TAX_RATE = 0.18
DEFAULT_CURRENCY = "USD"

# Table Names
TABLE_CUSTOMERS = "dim_customers"
TABLE_FULL_REFRESH = "orders_full_refresh"
TABLE_INCREMENTAL_MERGE = "orders_incremental_merge"
TABLE_PARTITION_REPLACE = "orders_partition_replace"
TABLE_AUDIT_LOG = "pipeline_audit_log"
