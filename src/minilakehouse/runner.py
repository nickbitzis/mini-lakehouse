"""The run loop: ingest bronze, build silver, test, gate, build gold, test, log."""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import duckdb

from . import bronze, quality
from .dag import LAYERS, REF_RE, Model, select
from .warehouse import columns, fingerprint, table_exists

VAR_RE = re.compile(r"\{\{\s*var\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
ANY_OF_RE = re.compile(r"\{\{\s*any_of\(([^)]*)\)\s*\}\}")
INC_RE = re.compile(r"\{\{\s*incremental_where\s*\}\}")


@dataclass
class ModelResult:
    model: str
    layer: str
    status: str  # success | error | blocked | skipped
    rows: int = 0
    rows_touched: int = 0
    partitions: tuple = ()
    duration_ms: float = 0.0
    fingerprint: str = ""
    message: str = ""
    data: str = ""  # same | changed | new, versus the last successful build


@dataclass
class RunReport:
    run_id: str
    command: str
    as_of: date
    status: str = "success"
    results: list[ModelResult] = field(default_factory=list)
    tests: list[quality.TestResult] = field(default_factory=list)
    gate: list[str] = field(default_factory=list)

    def result(self, model: str) -> ModelResult:
        return next(r for r in self.results if r.model == model)

    def to_text(self) -> str:
        lines = [f"run {self.run_id}  command={self.command}  as_of={self.as_of}  status={self.status}", ""]
        hdr = (f"{'model':<29}{'layer':<8}{'status':<9}{'rows':>7}{'written':>9}  {'data':<9}"
               f"{'partitions touched':<29}{'ms':>6}")
        lines += [hdr, "-" * len(hdr)]
        for r in self.results:
            lines.append(f"{r.model:<29}{r.layer:<8}{r.status:<9}{r.rows:>7}{r.rows_touched:>9}  {r.data or '-':<9}"
                         f"{_parts(r.partitions):<29}{r.duration_ms:>6.0f}")
        passed = sum(t.status == "pass" for t in self.tests)
        failed = sum(t.status in ("fail", "error") for t in self.tests)
        warned = sum(t.status == "warn" for t in self.tests)
        lines += ["", f"tests: {passed} passed, {failed} failed, {warned} warnings"]
        for t in self.tests:
            if t.status != "pass":
                lines.append(f"  {t.status.upper():<5} {t.model}.{t.test}: {t.message}")
        for r in self.results:
            if r.status in ("error", "skipped") and r.message:
                lines.append(f"  {r.status.upper():<5} {r.model}: {r.message}")
        if self.gate:
            lines += ["", "GATE FAILED: gold was not promoted. The previous gold tables are unchanged.", "Why:"]
            lines += [f"  - {g}" for g in self.gate]
        return "\n".join(lines)


def _parts(parts: tuple) -> str:
    if not parts:
        return "-"
    if len(parts) <= 2:
        return ",".join(parts)
    return f"{parts[0]}..{parts[-1]} ({len(parts)})"


def render(m: Model, con: duckdb.DuckDBPyConnection, *, variables: dict, incremental_where: str = "TRUE") -> str:
    """Expand ref(), var(), any_of() and incremental_where. Tiny on purpose: no Jinja."""
    sql = REF_RE.sub(lambda mm: mm.group(1), m.sql)
    sql = VAR_RE.sub(lambda mm: str(variables[mm.group(1)]), sql)
    sql = INC_RE.sub(incremental_where, sql)

    def any_of(mm: re.Match) -> str:
        # any_of('table', 'new_name', 'old_name'): COALESCE over whichever columns exist.
        args = [a.strip().strip("'\"") for a in mm.group(1).split(",")]
        table, cands = args[0], args[1:]
        have = set(columns(con, table)) if table_exists(con, table) else set()
        present = [f'"{c}"' for c in cands if c in have]
        if not present:
            return "NULL"
        return present[0] if len(present) == 1 else f"COALESCE({', '.join(present)})"

    return ANY_OF_RE.sub(any_of, sql)


def _max_seq(con) -> int:
    return con.execute("SELECT coalesce(max(batch_seq), 0) FROM _bronze_manifest").fetchone()[0]


def _distinct_parts(con, table: str, col: str | None) -> tuple:
    if not col:
        return ()
    rows = con.execute(f'SELECT DISTINCT "{col}"::VARCHAR FROM {table} WHERE "{col}" IS NOT NULL ORDER BY 1').fetchall()
    return tuple(r[0] for r in rows)


def _replace_table(con, name: str, sql: str) -> int:
    """Rebuild a table and return how many rows are new or changed versus the old version."""
    if not table_exists(con, name):
        con.execute(f"CREATE TABLE {name} AS {sql}")
        return con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    con.execute(f"CREATE OR REPLACE TEMP TABLE _old AS SELECT * FROM {name}")
    con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
    try:
        return con.execute(f"SELECT count(*) FROM (SELECT * FROM {name} EXCEPT ALL SELECT * FROM _old)").fetchone()[0]
    except duckdb.Error:  # the shape changed, so every row counts as changed
        return con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    finally:
        con.execute("DROP TABLE IF EXISTS _old")


def build_model(con, m: Model, *, variables: dict, full_refresh: bool = False,
                backfill: tuple[date, date] | None = None) -> tuple[int, tuple]:
    """Build one SQL model. Returns (rows touched, partitions touched)."""
    name = m.name
    if m.materialized == "table":
        return _replace_table(con, name, render(m, con, variables=variables)), ()
    if m.materialized != "incremental":
        raise ValueError(f"{name}: unknown materialization {m.materialized!r}")

    key = m.config["unique_key"]
    part = m.config.get("partition_by")
    conflict = m.config.get("on_conflict", "ignore")
    high = _max_seq(con)
    if not table_exists(con, name) or full_refresh or (backfill and not part):
        touched = _replace_table(con, name, render(m, con, variables=variables))
        parts = _distinct_parts(con, name, part)
    elif backfill:
        lo, hi = backfill
        con.execute(f"CREATE OR REPLACE TEMP TABLE _changed AS SELECT * FROM ({render(m, con, variables=variables)}) "
                    f"WHERE \"{part}\" BETWEEN DATE '{lo}' AND DATE '{hi}'")
        con.execute(f"DELETE FROM {name} WHERE \"{part}\" BETWEEN DATE '{lo}' AND DATE '{hi}'")
        con.execute(f"INSERT INTO {name} BY NAME SELECT * FROM _changed")
        touched = con.execute("SELECT count(*) FROM _changed").fetchone()[0]
        parts = _distinct_parts(con, "_changed", part)
    else:
        wm = con.execute("SELECT watermark FROM _model_state WHERE model = ?", [name]).fetchone()
        wm = wm[0] if wm else 0
        sql = render(m, con, variables=variables, incremental_where=f"_batch_seq > {wm}")
        con.execute(f"CREATE OR REPLACE TEMP TABLE _new AS {sql}")
        if conflict == "ignore":
            con.execute(f'CREATE OR REPLACE TEMP TABLE _changed AS SELECT n.* FROM _new n '
                        f'ANTI JOIN {name} t ON n."{key}" = t."{key}"')
        elif conflict.startswith("update_if_newer:"):
            col = conflict.split(":", 1)[1]
            con.execute(f'CREATE OR REPLACE TEMP TABLE _changed AS SELECT n.* FROM _new n '
                        f'LEFT JOIN {name} t ON n."{key}" = t."{key}" '
                        f'WHERE t."{key}" IS NULL OR n."{col}" > t."{col}"')
        else:
            raise ValueError(f"{name}: unknown on_conflict {conflict!r}")
        con.execute(f'DELETE FROM {name} WHERE "{key}" IN (SELECT "{key}" FROM _changed)')
        con.execute(f"INSERT INTO {name} BY NAME SELECT * FROM _changed")
        touched = con.execute("SELECT count(*) FROM _changed").fetchone()[0]
        parts = _distinct_parts(con, "_changed", part)
        con.execute("DROP TABLE _new")
    con.execute("INSERT OR REPLACE INTO _model_state VALUES (?, ?, now())", [name, high])
    return touched, parts


def _resolve_as_of(con, through: date | None) -> date:
    latest = con.execute("SELECT max(partition_date) FROM _bronze_manifest").fetchone()[0]
    if latest is None:
        return through or date.today()
    return min(latest, through) if through else latest


def run(con: duckdb.DuckDBPyConnection, home: Path | str, models: dict[str, Model], *,
        selection: list[str] | None = None, through: date | None = None, full_refresh: bool = False,
        backfill: tuple[date, date] | None = None, as_of: date | None = None, command: str = "run",
        variables: dict | None = None) -> RunReport:
    started = datetime.now()
    run_id = f"r{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    order = select(models, selection)
    report = RunReport(run_id, command, as_of or _resolve_as_of(con, through))
    broken: set[str] = set()  # models that errored or were skipped in this run

    for layer in LAYERS:
        names = [n for n in order if models[n].layer == layer]
        if not names:
            continue
        if layer == "gold" and report.gate:
            for n in names:
                kept = con.execute(f"SELECT count(*) FROM {n}").fetchone()[0] if table_exists(con, n) else 0
                report.results.append(ModelResult(n, layer, "blocked", rows=kept, data="kept",
                                                  message="quality gate failed upstream"))
                _log(con, run_id, report.results[-1], started)
            continue
        for n in names:
            m = models[n]
            bad = [d for d in m.deps if d in broken]
            if bad:
                broken.add(n)
                res = ModelResult(n, layer, "skipped", message=f"upstream {bad[0]} did not build")
            else:
                res = _execute(con, home, m, through=through, full_refresh=full_refresh, backfill=backfill,
                               variables={"as_of": report.as_of.isoformat(), **(variables or {})})
                if res.status != "success":
                    broken.add(n)
            report.results.append(res)
            _log(con, run_id, res, datetime.now())
        if layer == "bronze":
            report.as_of = as_of or _resolve_as_of(con, through)
        # Tests run after the whole layer is built.
        for n in names:
            if n in broken:
                continue
            for spec in models[n].tests:
                for s in quality.expand(spec):
                    tr = quality.run_test(con, n, s, as_of=report.as_of, run_id=run_id)
                    report.tests.append(tr)
                    con.execute("INSERT INTO _test_results VALUES (?, ?, ?, ?, ?, ?, ?)",
                                [run_id, tr.model, tr.test, tr.severity, tr.status, tr.failures, tr.message])
        if layer != "gold":
            report.gate += [f"{t.model}.{t.test}: {t.message}" for t in report.tests
                            if t.blocking and models[t.model].layer == layer]
            report.gate += [f"{n} did not build" for n in names if n in broken]

    statuses = {r.status for r in report.results}
    if report.gate or statuses & {"error", "blocked", "skipped"} or any(t.blocking for t in report.tests):
        report.status = "failed"
    elif any(t.status == "warn" for t in report.tests):
        report.status = "warn"
    con.execute("INSERT INTO _runs VALUES (?, ?, ?, now(), ?, ?, ?)",
                [run_id, command, started, report.as_of, report.status, "; ".join(report.gate)])
    return report


def _execute(con, home, m: Model, *, through, full_refresh, backfill, variables) -> ModelResult:
    t0 = time.perf_counter()
    res = ModelResult(m.name, m.layer, "success")
    try:
        if m.kind == "ingest":
            lr = bronze.ingest_source(con, Path(home), m.name, through=through, dates=backfill,
                                      force=bool(backfill))
            res.rows_touched, res.partitions = lr.rows, lr.partitions
            notes = [f"{lr.files_loaded} files loaded, {lr.files_skipped} already loaded"]
            if lr.new_columns:
                notes.append("schema drift: new columns " + ", ".join(lr.new_columns))
            res.message = "; ".join(notes)
        else:
            res.rows_touched, res.partitions = build_model(con, m, variables=variables, full_refresh=full_refresh,
                                                           backfill=backfill)
        if table_exists(con, m.name):
            res.rows = con.execute(f"SELECT count(*) FROM {m.name}").fetchone()[0]
            res.fingerprint = fingerprint(con, m.name) if m.kind != "ingest" else _bronze_fp(con, m.name)
            prev = con.execute("SELECT fingerprint FROM _run_log WHERE model = ? AND status = 'success' "
                               "ORDER BY started_at DESC LIMIT 1", [m.name]).fetchone()
            res.data = "new" if not prev else ("same" if prev[0] == res.fingerprint else "changed")
    except (duckdb.Error, ValueError, KeyError) as exc:
        res.status = "error"
        res.message = str(exc).splitlines()[0]
    res.duration_ms = (time.perf_counter() - t0) * 1000
    return res


def _bronze_fp(con, name: str) -> str:
    """Bronze rows carry load times, so fingerprint the manifest (file hashes) instead."""
    return con.execute("SELECT md5(coalesce(string_agg(file_hash, ',' ORDER BY source_file), '')) "
                       "FROM _bronze_manifest WHERE source = ?", [name]).fetchone()[0]


def _log(con, run_id: str, r: ModelResult, ts: datetime) -> None:
    con.execute("INSERT INTO _run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [run_id, r.model, r.layer, ts, r.duration_ms, r.rows, r.rows_touched, ",".join(r.partitions),
                 r.status, r.fingerprint, r.message])


def test_only(con, models: dict[str, Model], *, selection: list[str] | None = None,
              as_of: date | None = None) -> RunReport:
    """Run declared tests against the current tables without building anything."""
    started = datetime.now()
    run_id = f"t{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    report = RunReport(run_id, "test", as_of or _resolve_as_of(con, None))
    for n in select(models, selection):
        if not models[n].tests:
            continue
        if not table_exists(con, n):
            report.tests.append(quality.TestResult(n, "exists", "error", "error", 1, "table not built yet"))
            continue
        for spec in models[n].tests:
            for s in quality.expand(spec):
                tr = quality.run_test(con, n, s, as_of=report.as_of, run_id=run_id)
                report.tests.append(tr)
                con.execute("INSERT INTO _test_results VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [run_id, tr.model, tr.test, tr.severity, tr.status, tr.failures, tr.message])
    report.status = "failed" if any(t.status in ("fail", "error") for t in report.tests) else "success"
    con.execute("INSERT INTO _runs VALUES (?, 'test', ?, now(), ?, ?, '')",
                [run_id, started, report.as_of, report.status])
    return report
