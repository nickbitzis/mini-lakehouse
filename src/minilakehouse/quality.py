"""Data quality tests declared per model.

Each test returns a TestResult with the number of failing rows (or a 0/1 flag
for table-level checks). Severity ``error`` blocks promotion to the next layer;
``warn`` is reported but does not block.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb


@dataclass
class TestResult:
    model: str
    test: str
    severity: str
    status: str  # pass | fail | warn | error
    failures: int
    message: str = ""

    @property
    def blocking(self) -> bool:
        return self.status in ("fail", "error") and self.severity == "error"


def _rows(n: int) -> str:
    return f"{n} row" if n == 1 else f"{n} rows"


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _where(spec: dict) -> str:
    return f" AND ({spec['where']})" if spec.get("where") else ""


def expand(spec: dict) -> list[dict]:
    """``not_null`` with several columns becomes one test per column."""
    if spec["type"] == "not_null" and "columns" in spec:
        return [{**{k: v for k, v in spec.items() if k != "columns"}, "column": c} for c in spec["columns"]]
    return [spec]


def label(spec: dict) -> str:
    t = spec["type"]
    if t == "relationships":
        return f"relationships({spec['column']} -> {spec['to']}.{spec['field']})"
    if t == "row_count_anomaly":
        return "row_count_anomaly"
    if t == "freshness":
        return f"freshness({spec['column']} < {spec['max_age_hours']}h)"
    return f"{t}({spec.get('column', '')})"


def run_test(con: duckdb.DuckDBPyConnection, model: str, spec: dict, *, as_of: date | None = None,
             run_id: str | None = None) -> TestResult:
    t = spec["type"]
    sev = spec.get("severity", "error")
    name = t
    if t not in _CHECKS:
        return TestResult(model, t, sev, "error", 1, f"unknown test type {t!r}")
    try:
        name = label(spec)
        failures, msg = _CHECKS[t](con, model, spec, as_of=as_of, run_id=run_id)
    except KeyError as exc:
        return TestResult(model, t, sev, "error", 1, f"test is missing parameter {exc}")
    except duckdb.Error as exc:
        return TestResult(model, name, sev, "error", 1, f"test query failed: {exc}".splitlines()[0])
    status = "pass" if failures == 0 else ("fail" if sev == "error" else "warn")
    return TestResult(model, name, sev, status, int(failures), msg)


def _not_null(con, model, spec, **_):
    c = spec["column"]
    n = con.execute(f'SELECT count(*) FROM {model} WHERE "{c}" IS NULL{_where(spec)}').fetchone()[0]
    return n, f"{n} null values in {c}" if n else ""


def _unique(con, model, spec, **_):
    c = spec["column"]
    n = con.execute(f'SELECT count(*) FROM (SELECT "{c}" FROM {model} WHERE "{c}" IS NOT NULL{_where(spec)} '
                    f'GROUP BY 1 HAVING count(*) > 1)').fetchone()[0]
    return n, f"{n} duplicated values in {c}" if n else ""


def _accepted_values(con, model, spec, **_):
    c = spec["column"]
    vals = ", ".join(_q(v) for v in spec["values"])
    rows = con.execute(f'SELECT "{c}", count(*) FROM {model} WHERE "{c}" IS NOT NULL AND "{c}"::VARCHAR '
                       f'NOT IN ({vals}){_where(spec)} GROUP BY 1 ORDER BY 2 DESC').fetchall()
    n = sum(r[1] for r in rows)
    return n, (f"{_rows(n)} with unexpected {c}: " + ", ".join(str(r[0]) for r in rows[:5])) if n else ""


def _relationships(con, model, spec, **_):
    c, to, f = spec["column"], spec["to"], spec["field"]
    rows = con.execute(f'SELECT DISTINCT m."{c}" FROM {model} m ANTI JOIN {to} p ON m."{c}" = p."{f}" '
                       f'WHERE m."{c}" IS NOT NULL{_where(spec)} ORDER BY 1').fetchall()
    n = con.execute(f'SELECT count(*) FROM {model} m ANTI JOIN {to} p ON m."{c}" = p."{f}" '
                    f'WHERE m."{c}" IS NOT NULL{_where(spec)}').fetchone()[0]
    if not n:
        return 0, ""
    return n, f"{_rows(n)} point to missing {to}.{f}: " + ", ".join(str(r[0]) for r in rows[:5])


def _freshness(con, model, spec, *, as_of=None, **_):
    c, hours = spec["column"], float(spec["max_age_hours"])
    if as_of is None:
        as_of = date.today()
    latest = con.execute(f'SELECT max("{c}")::TIMESTAMP FROM {model}').fetchone()[0]
    if latest is None:
        return 1, "table is empty"
    age = con.execute(f"SELECT epoch(DATE '{as_of.isoformat()}' + INTERVAL 1 DAY - ?::TIMESTAMP) / 3600.0",
                      [latest]).fetchone()[0]
    if age > hours:
        return 1, f"latest {c} is {latest}, {age:.0f}h old at end of {as_of} (limit {hours:.0f}h)"
    return 0, ""


def _row_count_anomaly(con, model, spec, *, run_id=None, **_):
    current = con.execute(f"SELECT count(*) FROM {model}").fetchone()[0]
    prev = con.execute(
        "SELECT rows FROM _run_log WHERE model = ? AND status = 'success' AND run_id <> ? "
        "ORDER BY started_at DESC LIMIT 1", [model, run_id or ""]).fetchone()
    if not prev or not prev[0]:
        return 0, ""
    prev = prev[0]
    change = (current - prev) / prev * 100
    max_drop = float(spec.get("max_drop_pct", 50))
    max_growth = spec.get("max_growth_pct")
    if -change > max_drop:
        return 1, f"rows dropped {-change:.0f}% ({prev} -> {current}), limit {max_drop:.0f}%"
    if max_growth is not None and change > float(max_growth):
        return 1, f"rows grew {change:.0f}% ({prev} -> {current}), limit {float(max_growth):.0f}%"
    return 0, ""


_CHECKS = {
    "not_null": _not_null,
    "unique": _unique,
    "accepted_values": _accepted_values,
    "relationships": _relationships,
    "freshness": _freshness,
    "row_count_anomaly": _row_count_anomaly,
}
