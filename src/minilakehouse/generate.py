"""Synthetic sources for a made-up subscription business, Larkspur Analytics.

Three sources land files into daily partitions under ``<home>/landing``:

* crm:      a full customer snapshot per day as CSV.
* billing:  JSON lines of billing events, with late arrivals, duplicates, and a
            field rename (``amount_cents`` becomes ``amount_minor``) partway through.
* product:  product usage events as Parquet, written with DuckDB.

Day 1 of billing carries the opening history (six months of subscriptions and
invoices), the way a new source looks when it is first connected.
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

import duckdb

PLANS = {"starter": 2900, "team": 9900, "enterprise": 49900}
SEGMENTS = ["smb", "mid_market", "enterprise"]
COUNTRIES = ["US", "CA", "GB", "DE", "AU", "NL"]
PRODUCT_EVENTS = ["login", "view_dashboard", "export_report", "invite_user", "create_alert"]
NAME_A = ["Copper", "Harbor", "Juniper", "Northwind", "Quill", "Saltmarsh", "Tidewater", "Vantage", "Willow"]
NAME_B = ["Labs", "Works", "Freight", "Health", "Studio", "Foods", "Logistics", "Media", "Supply"]

DEFAULT_START = date(2026, 10, 27)
DRIFT_DAY_INDEX = 3  # from the 4th day on, billing renames amount_cents -> amount_minor


@dataclass
class Sub:
    sub_id: str
    customer_id: str
    plan: str
    start: datetime
    end: datetime | None = None


@dataclass
class World:
    rng: random.Random
    customers: dict = field(default_factory=dict)
    subs: list = field(default_factory=list)
    next_customer: int = 1
    next_event: int = 1


def _ts(d: date, rng: random.Random) -> datetime:
    return datetime.combine(d, time(0, 0)) + timedelta(seconds=rng.randint(0, 86399))


def _new_customer(w: World, created: datetime) -> str:
    cid = f"C{w.next_customer:05d}"
    w.next_customer += 1
    w.customers[cid] = {
        "customer_id": cid,
        "company_name": f"{w.rng.choice(NAME_A)} {w.rng.choice(NAME_B)} {cid[-3:]}",
        "country": w.rng.choice(COUNTRIES),
        "segment": w.rng.choice(SEGMENTS),
        "created_at": created.isoformat(timespec="seconds"),
        "updated_at": created.isoformat(timespec="seconds"),
    }
    return cid


def _event(w: World, etype: str, ts: datetime, sub: Sub, amount: int | None) -> dict:
    eid = f"evt_{w.next_event:07d}"
    w.next_event += 1
    return {
        "event_id": eid,
        "event_type": etype,
        "event_ts": ts.isoformat(timespec="seconds"),
        "customer_id": sub.customer_id,
        "subscription_id": sub.sub_id,
        "plan": sub.plan,
        "amount": amount,
        "currency": "USD",
    }


def _invoices_between(w: World, sub: Sub, lo: datetime, hi: datetime) -> list[dict]:
    """Monthly invoices on the subscription anniversary that fall in [lo, hi)."""
    out = []
    k = 0
    while True:
        m = sub.start.month - 1 + k
        y = sub.start.year + m // 12
        day = min(sub.start.day, 28)
        ts = sub.start.replace(year=y, month=m % 12 + 1, day=day)
        k += 1
        if ts >= hi or (sub.end and ts >= sub.end):
            break
        if ts >= lo:
            out.append(_event(w, "invoice_paid", ts, sub, PLANS[sub.plan]))
    return out


def generate(home: Path, days: int = 7, start: date = DEFAULT_START, seed: int = 7,
             corrupt_last_day: bool = False) -> dict:
    """Write ``days`` daily partitions for all three sources. Returns a summary."""
    rng = random.Random(seed)
    w = World(rng=rng)
    landing = Path(home) / "landing"
    history_start = datetime.combine(start - timedelta(days=180), time(0))
    day0 = datetime.combine(start, time(0))

    # Opening history: customers and subscriptions over the previous 180 days.
    history_events: list[dict] = []
    for _ in range(120):
        created = history_start + timedelta(seconds=rng.randint(0, 180 * 86400 - 1))
        cid = _new_customer(w, created)
        if rng.random() < 0.9:
            sub = Sub(f"S{len(w.subs) + 1:05d}", cid, rng.choices(list(PLANS), [6, 3, 1])[0], created)
            if rng.random() < 0.15:
                sub.end = created + timedelta(days=rng.randint(20, 150))
                if sub.end >= day0:
                    sub.end = None
            w.subs.append(sub)
    for sub in w.subs:
        history_events.append(_event(w, "subscription_created", sub.start, sub, PLANS[sub.plan]))
        history_events.extend(_invoices_between(w, sub, history_start, day0))
        if sub.end:
            history_events.append(_event(w, "subscription_canceled", sub.end, sub, None))
    # Reorder ids so they follow time, the way a real event log would.
    history_events.sort(key=lambda e: e["event_ts"])

    billing_by_day: dict[int, list[dict]] = {i: [] for i in range(days)}
    billing_by_day[0].extend(history_events)
    late = dupes = 0
    summary_days = []

    for i in range(days):
        d = start + timedelta(days=i)
        lo = datetime.combine(d, time(0))
        hi = lo + timedelta(days=1)
        todays: list[dict] = []
        # New customers, most of whom subscribe the same day.
        for _ in range(rng.randint(2, 5)):
            created = _ts(d, rng)
            cid = _new_customer(w, created)
            if rng.random() < 0.8:
                sub = Sub(f"S{len(w.subs) + 1:05d}", cid, rng.choices(list(PLANS), [6, 3, 1])[0], created)
                w.subs.append(sub)
                todays.append(_event(w, "subscription_created", created, sub, PLANS[sub.plan]))
        # A few CRM edits (segment changes).
        for cid in rng.sample(sorted(w.customers), k=3):
            c = w.customers[cid]
            if c["created_at"] < lo.isoformat():
                c["segment"] = rng.choice(SEGMENTS)
                c["updated_at"] = _ts(d, rng).isoformat(timespec="seconds")
        # Cancellations and renewals.
        active = [s for s in w.subs if s.end is None and s.start < lo]
        for sub in rng.sample(active, k=min(len(active), rng.randint(0, 2))):
            sub.end = _ts(d, rng)
            todays.append(_event(w, "subscription_canceled", sub.end, sub, None))
        for sub in w.subs:
            if sub.start < hi:
                todays.extend(_invoices_between(w, sub, lo, hi))

        # Late arrivals: about 6% of today's events land one or two days later.
        for e in todays:
            shift = 0
            if rng.random() < 0.06 and i + 1 < days:
                shift = min(days - 1 - i, rng.choice([1, 2]))
                late += 1
            billing_by_day[i + shift].append(e)
            # Duplicates: about 4% are delivered twice, same file or the next one.
            if rng.random() < 0.04:
                dupes += 1
                billing_by_day[min(days - 1, i + shift + rng.choice([0, 1]))].append(dict(e))

        # CRM snapshot as of end of day.
        crm_dir = landing / "crm" / f"dt={d.isoformat()}"
        crm_dir.mkdir(parents=True, exist_ok=True)
        cols = ["customer_id", "company_name", "country", "segment", "created_at", "updated_at"]
        snapshot = [c for c in w.customers.values() if c["created_at"] < hi.isoformat()]
        with open(crm_dir / "customers.csv", "w", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=cols)
            wr.writeheader()
            wr.writerows(sorted(snapshot, key=lambda c: c["customer_id"]))

        # Product events for accounts with an active subscription.
        live = sorted({s.customer_id for s in w.subs if s.start < hi and (s.end is None or s.end > lo)})
        rows = []
        for cid in live:
            if rng.random() < 0.6:
                for _ in range(rng.randint(1, 12)):
                    rows.append((f"pe_{w.next_event:07d}", cid, rng.choice(PRODUCT_EVENTS), _ts(d, rng)))
                    w.next_event += 1
        rows += [r for r in rows if rng.random() < 0.01]  # at-least-once delivery: a few repeats
        _write_parquet(landing / "product" / f"dt={d.isoformat()}" / "events.parquet", rows)
        summary_days.append({"date": d.isoformat(), "customers": len(snapshot), "product_events": len(rows)})

    if corrupt_last_day:
        # A bad upstream deploy: events for customers that do not exist and an unknown event type.
        ghost = Sub("S99999", "C99999", "team", datetime.combine(start + timedelta(days=days - 1), time(9)))
        billing_by_day[days - 1].append(_event(w, "invoice_paid", ghost.start, ghost, 9900))
        billing_by_day[days - 1].append(_event(w, "invoice_refunded_v2", ghost.start, ghost, -9900))

    for i in range(days):
        d = start + timedelta(days=i)
        bdir = landing / "billing" / f"dt={d.isoformat()}"
        bdir.mkdir(parents=True, exist_ok=True)
        amount_field = "amount_minor" if i >= DRIFT_DAY_INDEX else "amount_cents"
        with open(bdir / "events.jsonl", "w") as fh:
            for e in billing_by_day[i]:
                rec = {k: v for k, v in e.items() if k != "amount"}
                rec[amount_field] = e["amount"]
                fh.write(json.dumps(rec) + "\n")
        summary_days[i]["billing_events"] = len(billing_by_day[i])

    return {"start": start.isoformat(), "days": days, "seed": seed, "late_events": late,
            "duplicate_events": dupes, "drift_from": (start + timedelta(days=DRIFT_DAY_INDEX)).isoformat(),
            "per_day": summary_days}


def _write_parquet(path: Path, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE t (event_id VARCHAR, account_id VARCHAR, event_name VARCHAR, event_ts TIMESTAMP)")
    if rows:
        con.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
    con.execute(f"COPY (SELECT * FROM t ORDER BY event_ts) TO '{path.as_posix()}' (FORMAT parquet)")
    con.close()
