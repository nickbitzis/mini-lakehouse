"""Command line entry point: ``minilakehouse <command>``."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from . import generate as gen
from . import lineage, runner
from .dag import load_models, select
from .warehouse import connect, default_home


def _date(s: str) -> date:
    return date.fromisoformat(s)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="minilakehouse", description="A small medallion lakehouse on DuckDB.")
    p.add_argument("--home", type=Path, default=None,
                   help="data folder (default: $MINILAKEHOUSE_HOME or ./lakehouse)")
    p.add_argument("--models", type=Path, default=None, help="models folder (default: bundled models)")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write synthetic landing files")
    g.add_argument("--days", type=int, default=7)
    g.add_argument("--start", type=_date, default=gen.DEFAULT_START)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--corrupt-last-day", action="store_true",
                   help="add billing events for an unknown customer and an unknown event type")

    r = sub.add_parser("run", help="ingest new files and build models")
    r.add_argument("--select", "-s", action="append", help="model, model+, +model, or layer:silver")
    r.add_argument("--through", type=_date, help="only ingest landing partitions up to this date")
    r.add_argument("--full-refresh", action="store_true", help="rebuild incremental models from scratch")

    t = sub.add_parser("test", help="run data quality tests against current tables")
    t.add_argument("--select", "-s", action="append")

    b = sub.add_parser("backfill", help="reload and rebuild a date range of partitions")
    b.add_argument("--from", dest="start", type=_date, required=True)
    b.add_argument("--to", dest="end", type=_date, required=True)

    li = sub.add_parser("lineage", help="write docs/lineage.mmd and docs/lineage.html")
    li.add_argument("--out", type=Path, default=Path("docs"))

    sub.add_parser("ls", help="list models in run order")
    sub.add_parser("runs", help="show the run log for the latest runs")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    home = args.home or default_home()

    if args.command == "generate":
        summary = gen.generate(home, days=args.days, start=args.start, seed=args.seed,
                               corrupt_last_day=args.corrupt_last_day)
        print(json.dumps({k: v for k, v in summary.items() if k != "per_day"}))
        for d in summary["per_day"]:
            print(f"  {d['date']}: {d['customers']} customers, {d['billing_events']} billing events, "
                  f"{d['product_events']} product events")
        return 0

    models = load_models(args.models)
    if args.command == "ls":
        for n in select(models, None):
            m = models[n]
            print(f"{m.layer:<7} {n:<28} deps: {', '.join(m.deps) or '-'}")
        return 0

    con = connect(home)
    try:
        if args.command == "run":
            rep = runner.run(con, home, models, selection=args.select, through=args.through,
                             full_refresh=args.full_refresh)
        elif args.command == "backfill":
            if args.end < args.start:
                print("--to must be on or after --from", file=sys.stderr)
                return 2
            rep = runner.run(con, home, models, backfill=(args.start, args.end), command="backfill",
                             as_of=None)
        elif args.command == "test":
            rep = runner.test_only(con, models, selection=args.select)
        elif args.command == "lineage":
            out = lineage.export(con, models, args.out)
            print(f"wrote {out['mermaid']} and {out['html']} ({out['models']} models)")
            return 0
        elif args.command == "runs":
            rows = con.execute("SELECT run_id, model, status, rows, rows_touched, round(duration_ms) "
                               "FROM _run_log ORDER BY started_at DESC LIMIT 40").fetchall()
            for r in rows:
                print(f"{r[0]:<26} {r[1]:<28} {r[2]:<8} rows={r[3]:<7} touched={r[4]:<6} {r[5]:.0f}ms")
            return 0
        else:  # pragma: no cover
            return 2
        print(rep.to_text())
        return 0 if rep.status in ("success", "warn") else 1
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
