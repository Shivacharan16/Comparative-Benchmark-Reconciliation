"""
Pytest fixtures for Ingestion Strategy comparison tests.
"""
import pytest
import duckdb
from src.generator.data_generator import DatasetGenerator
from src.validator.validator import BatchValidator
from src.pipelines.full_refresh import FullRefreshPipeline
from src.pipelines.incremental_merge import IncrementalMergePipeline
from src.pipelines.partition_replace import PartitionReplacementPipeline
from src.audit.audit_logger import AuditLogger

@pytest.fixture
def db_conn():
    """Provides an isolated in-memory DuckDB connection for each test."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()

@pytest.fixture
def sample_customers():
    """Provides a list of deterministic sample customer records."""
    generator = DatasetGenerator(seed=101)
    return generator.generate_customers(count=50)

@pytest.fixture
def seeded_pipelines(db_conn, sample_customers):
    """Sets up in-memory pipelines with populated customer dimension."""
    p_full = FullRefreshPipeline(db_conn, "test_orders_full")
    p_inc = IncrementalMergePipeline(db_conn, "test_orders_inc")
    p_part = PartitionReplacementPipeline(db_conn, "test_orders_part")

    p_full.load_customers(sample_customers)
    p_inc.load_customers(sample_customers)
    p_part.load_customers(sample_customers)

    return {
        "full": p_full,
        "inc": p_inc,
        "part": p_part,
        "customers": sample_customers,
        "conn": db_conn
    }
