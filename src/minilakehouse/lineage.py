"""Lineage export: a Mermaid diagram and a static HTML page with latest run stats."""

from __future__ import annotations

import html
import json
from pathlib import Path

import duckdb

from .bronze import SOURCES
from .dag import LAYERS, Model, topo_sort
from .warehouse import columns, table_exists


def mermaid(models: dict[str, Model], stats: dict[str, dict] | None = None) -> str:
    stats = stats or {}
    lines = ["flowchart LR"]
    lines.append("  subgraph landing")
    for name, src in SOURCES.items():
        if name in models:
            lines.append(f'    src_{src.folder}[("{src.folder} ({src.fmt})")]')
    lines.append("  end")
    for layer in LAYERS:
        names = [n for n in topo_sort(models) if models[n].layer == layer]
        if not names:
            continue
        lines.append(f"  subgraph {layer}")
        for n in names:
            s = stats.get(n)
            extra = f"<br/>{s['rows']} rows, {s['status']}" if s else ""
            lines.append(f'    {n}["{n}{extra}"]')
        lines.append("  end")
    for name, src in SOURCES.items():
        if name in models:
            lines.append(f"  src_{src.folder} --> {name}")
    for n in topo_sort(models):
        for d in models[n].deps:
            lines.append(f"  {d} --> {n}")
    for n, s in stats.items():
        if s.get("status") in ("error", "blocked", "skipped") or s.get("tests_failed"):
            lines.append(f"  style {n} stroke:#c0392b,stroke-width:2px")
    return "\n".join(lines) + "\n"


def latest_stats(con: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    """Latest logged run per model, joined with its test results from that run."""
    rows = con.execute("""
        WITH latest AS (
            SELECT * FROM _run_log
            QUALIFY row_number() OVER (PARTITION BY model ORDER BY started_at DESC) = 1
        )
        SELECT l.model, l.run_id, l.status, l.rows, l.rows_touched, l.partitions_touched, l.duration_ms,
               l.started_at, l.message,
               count(t.test) FILTER (WHERE t.status = 'pass') AS passed,
               count(t.test) FILTER (WHERE t.status IN ('fail', 'error')) AS failed,
               count(t.test) FILTER (WHERE t.status = 'warn') AS warned
        FROM latest l
        LEFT JOIN _test_results t ON t.run_id = l.run_id AND t.model = l.model
        GROUP BY ALL
    """).fetchall()
    out = {}
    for r in rows:
        out[r[0]] = {"run_id": r[1], "status": r[2], "rows": r[3], "rows_touched": r[4],
                     "partitions_touched": r[5], "duration_ms": round(r[6] or 0, 1), "started_at": str(r[7]),
                     "message": r[8] or "", "tests_passed": r[9], "tests_failed": r[10], "tests_warned": r[11]}
    return out


def export(con: duckdb.DuckDBPyConnection, models: dict[str, Model], out_dir: Path | str = "docs") -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stats = latest_stats(con)
    mmd = mermaid(models, stats)
    (out / "lineage.mmd").write_text(mmd)
    runs = con.execute("SELECT run_id, command, started_at, as_of, status, message FROM _runs "
                       "ORDER BY started_at DESC LIMIT 10").fetchall()
    drift = con.execute("SELECT source, column_name, change, batch_id FROM _schema_changes "
                        "ORDER BY detected_at").fetchall()
    (out / "lineage.html").write_text(_html(models, stats, mmd, runs, drift, con))
    return {"mermaid": str(out / "lineage.mmd"), "html": str(out / "lineage.html"), "models": len(models)}


def _html(models, stats, mmd, runs, drift, con) -> str:
    e = html.escape
    rows = []
    for n in topo_sort(models):
        m, s = models[n], stats.get(n, {})
        cols = columns(con, n) if table_exists(con, n) else []
        tests = (f"{s.get('tests_passed', 0)}/{s.get('tests_passed', 0) + s.get('tests_failed', 0)}"
                 if m.tests else "-")
        cls = "bad" if s.get("status") in ("error", "blocked", "skipped") or s.get("tests_failed") else ""
        rows.append(
            f"<tr class='{cls}'><td><code>{e(n)}</code></td><td>{m.layer}</td>"
            f"<td>{e(m.materialized if m.kind == 'sql' else 'ingest')}</td>"
            f"<td>{e(', '.join(m.deps) or '-')}</td><td>{e(str(s.get('status', 'not run')))}</td>"
            f"<td class='num'>{s.get('rows', '')}</td><td class='num'>{s.get('rows_touched', '')}</td>"
            f"<td class='num'>{s.get('duration_ms', '')}</td><td class='num'>{tests}</td>"
            f"<td class='cols'>{e(', '.join(c for c in cols if not c.startswith('_')))}</td></tr>")
    run_rows = "".join(
        f"<tr><td><code>{e(r[0])}</code></td><td>{e(r[1])}</td><td>{e(str(r[2])[:19])}</td><td>{r[3]}</td>"
        f"<td>{e(r[4])}</td><td>{e(r[5] or '')}</td></tr>" for r in runs)
    drift_rows = "".join(f"<tr><td>{e(d[0])}</td><td><code>{e(d[1])}</code></td><td>{e(d[2])}</td>"
                         f"<td><code>{e(d[3])}</code></td></tr>" for d in drift) or \
        "<tr><td colspan='4'>No schema changes detected.</td></tr>"
    data = json.dumps(stats, default=str)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Lakehouse Lineage</title>
<style>
:root {{ --bg:#fbfaf7; --fg:#1d232b; --muted:#5d6672; --line:#dcd9d2; --card:#ffffff; --bad:#c0392b; --accent:#2f6f8f; }}
@media (prefers-color-scheme: dark) {{
  :root {{ --bg:#14171b; --fg:#e6e8eb; --muted:#9aa3ad; --line:#2c3238; --card:#1b1f24; --bad:#ef7a6a; --accent:#7ab8d6; }}
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--fg); font:15px/1.5 system-ui,-apple-system,sans-serif; }}
main {{ max-width:1200px; margin:0 auto; padding:24px 16px 64px; }}
h1 {{ font-size:22px; margin:0 0 4px; }} h2 {{ font-size:17px; margin:32px 0 8px; }}
p.lede {{ color:var(--muted); margin:0 0 16px; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:8px; padding:12px; overflow-x:auto; }}
table {{ border-collapse:collapse; width:100%; font-size:13px; }}
th,td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); vertical-align:top; }}
th {{ color:var(--muted); font-weight:600; }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
td.cols {{ color:var(--muted); max-width:340px; }}
tr.bad td:first-child {{ border-left:3px solid var(--bad); }}
code {{ font-size:12px; }}
.mermaid {{ min-width:720px; }}
</style>
</head>
<body>
<main>
<h1>Lakehouse lineage</h1>
<p class="lede">Model DAG from landing files through bronze, silver, and gold, with stats from each model's latest run.
Red outlines mark models whose latest run failed, was blocked, or had failing tests.</p>
<div class="card"><pre class="mermaid">{e(mmd)}</pre></div>
<h2>Models</h2>
<div class="card"><table>
<thead><tr><th>model</th><th>layer</th><th>build</th><th>depends on</th><th>status</th><th>rows</th>
<th>written</th><th>ms</th><th>tests</th><th>columns</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div>
<h2>Recent runs</h2>
<div class="card"><table><thead><tr><th>run</th><th>command</th><th>started</th><th>as of</th><th>status</th>
<th>gate</th></tr></thead><tbody>{run_rows}</tbody></table></div>
<h2>Schema changes</h2>
<div class="card"><table><thead><tr><th>source</th><th>column</th><th>change</th><th>batch</th></tr></thead>
<tbody>{drift_rows}</tbody></table></div>
</main>
<script type="application/json" id="stats">{e(data)}</script>
<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs";
const dark = window.matchMedia("(prefers-color-scheme: dark)").matches;
mermaid.initialize({{ startOnLoad: true, theme: dark ? "dark" : "neutral", securityLevel: "strict" }});
</script>
</body>
</html>
"""
