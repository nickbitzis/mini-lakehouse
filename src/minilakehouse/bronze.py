"""Bronze: raw, append-only copies of landing files with load metadata.

Every landing file becomes one batch. A manifest records each file's hash, so
loading the same file twice is a no-op. A changed file (a corrected re-delivery)
replaces its earlier batch. New columns are added to the bronze table and logged
in ``_schema_changes`` instead of failing the load.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from .warehouse import columns, table_exists


@dataclass(frozen=True)
class Source:
    name: str  # bronze table name
    folder: str  # folder under landing/
    filename: str
    reader: str  # duckdb table function call template, {path} is replaced
    fmt: str


SOURCES = {
    "bronze_crm_customers": Source(
        "bronze_crm_customers", "crm", "customers.csv",
        "read_csv('{path}', all_varchar = true, header = true, hive_partitioning = false)", "CSV"),
    "bronze_billing_events": Source(
        "bronze_billing_events", "billing", "events.jsonl",
        "read_json('{path}', format = 'newline_delimited', hive_partitioning = false)", "JSON lines"),
    "bronze_product_events": Source("bronze_product_events", "product", "events.parquet",
                                    "read_parquet('{path}', hive_partitioning = false)", "Parquet"),
}

META_COLS = ["_source_file", "_loaded_at", "_batch_id", "_batch_seq", "_partition_date"]


@dataclass
class LoadResult:
    source: str
    files_loaded: int = 0
    files_skipped: int = 0
    rows: int = 0
    partitions: tuple = ()
    new_columns: tuple = ()


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def landing_files(home: Path, src: Source) -> list[tuple[date, Path]]:
    out = []
    for p in sorted((Path(home) / "landing" / src.folder).glob(f"dt=*/{src.filename}")):
        out.append((date.fromisoformat(p.parent.name[3:]), p))
    return out


def ingest_source(con: duckdb.DuckDBPyConnection, home: Path, source: str, *,
                  through: date | None = None, dates: tuple[date, date] | None = None,
                  force: bool = False) -> LoadResult:
    src = SOURCES[source]
    res = LoadResult(source)
    partitions, new_cols = [], []
    for dt, path in landing_files(home, src):
        if through and dt > through:
            continue
        if dates and not (dates[0] <= dt <= dates[1]):
            continue
        rel = path.relative_to(home).as_posix()
        digest = _hash(path)
        prev = con.execute("SELECT file_hash FROM _bronze_manifest WHERE source_file = ?", [rel]).fetchone()
        if prev and prev[0] == digest and not force:
            res.files_skipped += 1
            continue
        if prev:  # corrected re-delivery or forced backfill: replace the old batch
            if table_exists(con, src.name):
                con.execute(f"DELETE FROM {src.name} WHERE _source_file = ?", [rel])
            con.execute("DELETE FROM _bronze_manifest WHERE source_file = ?", [rel])
        seq = con.execute("SELECT coalesce(max(batch_seq), 0) + 1 FROM _bronze_manifest").fetchone()[0]
        batch_id = f"{src.folder}-{dt.isoformat()}-{digest[:8]}"
        con.execute(f"CREATE OR REPLACE TEMP TABLE _incoming AS SELECT COLUMNS(*)::VARCHAR "
                    f"FROM {src.reader.format(path=path.as_posix())}")
        incoming = columns(con, "_incoming")
        if not table_exists(con, src.name):
            cols = ", ".join(f'"{c}" VARCHAR' for c in incoming)
            con.execute(f"CREATE TABLE {src.name} ({cols}, _source_file VARCHAR, _loaded_at TIMESTAMP, "
                        f"_batch_id VARCHAR, _batch_seq BIGINT, _partition_date DATE)")
        else:
            existing = set(columns(con, src.name))
            for c in incoming:
                if c not in existing:
                    con.execute(f'ALTER TABLE {src.name} ADD COLUMN "{c}" VARCHAR')
                    con.execute("INSERT INTO _schema_changes VALUES (?, ?, 'added', ?, now())",
                                [src.name, c, batch_id])
                    new_cols.append(c)
            for c in existing - set(incoming) - set(META_COLS):
                already = con.execute("SELECT count(*) FROM _schema_changes WHERE source = ? AND column_name = ? "
                                      "AND change = 'missing'", [src.name, c]).fetchone()[0]
                if not already:
                    con.execute("INSERT INTO _schema_changes VALUES (?, ?, 'missing', ?, now())",
                                [src.name, c, batch_id])
        n = con.execute("SELECT count(*) FROM _incoming").fetchone()[0]
        con.execute(f"INSERT INTO {src.name} BY NAME SELECT *, ? AS _source_file, ? AS _loaded_at, "
                    f"? AS _batch_id, ? AS _batch_seq, ? AS _partition_date FROM _incoming",
                    [rel, datetime.now(), batch_id, seq, dt])
        con.execute("INSERT INTO _bronze_manifest VALUES (?, ?, ?, ?, ?, ?, ?, now())",
                    [src.name, rel, digest, dt, batch_id, seq, n])
        con.execute("DROP TABLE _incoming")
        res.files_loaded += 1
        res.rows += n
        partitions.append(dt.isoformat())
    res.partitions = tuple(partitions)
    res.new_columns = tuple(new_cols)
    return res
