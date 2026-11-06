"""Warehouse connection and the platform's own metadata tables."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

META_DDL = [
    """CREATE TABLE IF NOT EXISTS _bronze_manifest (
        source VARCHAR, source_file VARCHAR, file_hash VARCHAR, partition_date DATE,
        batch_id VARCHAR, batch_seq BIGINT, rows BIGINT, loaded_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS _schema_changes (
        source VARCHAR, column_name VARCHAR, change VARCHAR, batch_id VARCHAR, detected_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS _model_state (model VARCHAR PRIMARY KEY, watermark BIGINT, updated_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS _runs (
        run_id VARCHAR, command VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
        as_of DATE, status VARCHAR, message VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS _run_log (
        run_id VARCHAR, model VARCHAR, layer VARCHAR, started_at TIMESTAMP, duration_ms DOUBLE,
        rows BIGINT, rows_touched BIGINT, partitions_touched VARCHAR, status VARCHAR,
        fingerprint VARCHAR, message VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS _test_results (
        run_id VARCHAR, model VARCHAR, test VARCHAR, severity VARCHAR, status VARCHAR,
        failures BIGINT, message VARCHAR)""",
]


def default_home() -> Path:
    return Path(os.environ.get("MINILAKEHOUSE_HOME", "lakehouse"))


def connect(home: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """Open the warehouse file under ``home`` (or an in-memory one for ``":memory:"``)."""
    if home == ":memory:":
        con = duckdb.connect()
    else:
        home = Path(home or default_home())
        home.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(home / "warehouse.duckdb"))
    ensure_meta(con)
    return con


def ensure_meta(con: duckdb.DuckDBPyConnection) -> None:
    for ddl in META_DDL:
        con.execute(ddl)


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()[0])


def columns(con: duckdb.DuckDBPyConnection, name: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
        [name]).fetchall()
    return [r[0] for r in rows]


def fingerprint(con: duckdb.DuckDBPyConnection, name: str) -> str:
    """Order-independent content hash of a table, used to prove reruns change nothing."""
    return con.execute(
        f"SELECT md5(coalesce(string_agg(x, '|' ORDER BY x), '')) FROM (SELECT t::VARCHAR AS x FROM {name} t)"
    ).fetchone()[0]
