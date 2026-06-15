"""src/streaming/storage/storage_weather.py."""

from pathlib import Path
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.storage.duckdb_sql import (
    build_clear_table_sql,
    build_create_table_sql,
    build_insert_sql,
)
from datafun_toolkit.logger import get_logger
import duckdb

from streaming.data_validation.data_contract_weather import (
    CONSUMED_FIELDNAMES,
    REJECTED_FIELDNAMES,
)
from streaming.data_validation.data_validation_case import add_validation_errors

LOG = get_logger("W-STORAGE", level="DEBUG")

VALID_TABLE_NAME: Final[str] = "weather_valid"
REJECTED_TABLE_NAME: Final[str] = "weather_rejected"

CONSUMED_REJECTED_FIELDNAMES: Final[list[str]] = [
    *REJECTED_FIELDNAMES,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]


def clean_valid_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the valid consumed table."""
    return {field: record.get(field, "") for field in CONSUMED_FIELDNAMES}


def clean_rejected_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the rejected consumed table."""
    return {field: record.get(field, "") for field in CONSUMED_REJECTED_FIELDNAMES}


def create_storage_tables(db_path: Path) -> None:
    """Create the consumed message tables if they do not exist."""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(build_create_table_sql(VALID_TABLE_NAME, CONSUMED_FIELDNAMES))
        conn.execute(
            build_create_table_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
        )


def clear_storage_tables(db_path: Path) -> None:
    """Clear prior consumed message rows for a fresh run."""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(build_clear_table_sql(VALID_TABLE_NAME))
        conn.execute(build_clear_table_sql(REJECTED_TABLE_NAME))


def init_db(db_path: Path) -> None:
    """Initialize the DuckDB database for this project."""
    create_storage_tables(db_path)
    clear_storage_tables(db_path)


def write_valid_record(db_path: Path, record: DataRecordDict) -> None:
    """Write one valid consumed weather record to DuckDB."""
    clean_record = clean_valid_record(record)
    insert_sql = build_insert_sql(VALID_TABLE_NAME, CONSUMED_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def write_rejected_record(
    db_path: Path, record: DataRecordDict, errors: list[str]
) -> None:
    """Write one rejected consumed weather record to DuckDB."""
    rejected_record = add_validation_errors(record=record, errors=errors)
    clean_record = clean_rejected_record(rejected_record)
    insert_sql = build_insert_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_REJECTED_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def log_storage_summary(db_path: Path) -> None:
    """Log simple DuckDB query results after consuming messages."""
    sql_valid_count = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
    sql_rejected_count = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608
    sql_avg_temp = f"SELECT AVG(CAST(temperature_f AS FLOAT)) FROM {VALID_TABLE_NAME}"  # noqa: S608

    with duckdb.connect(str(db_path)) as conn:
        valid_result = conn.execute(sql_valid_count).fetchone()
        valid_count = valid_result[0] if valid_result else 0

        rejected_result = conn.execute(sql_rejected_count).fetchone()
        rejected_count = rejected_result[0] if rejected_result else 0

        avg_temp_res = conn.execute(sql_avg_temp).fetchone()
        avg_temp = avg_temp_res[0] if avg_temp_res and avg_temp_res[0] else 0.0

    LOG.info(f"DuckDB Valid rows: {valid_count}")
    LOG.info(f"DuckDB Rejected rows: {rejected_count}")
    LOG.info(f"DuckDB Average Temp (F): {avg_temp:.2f}")
