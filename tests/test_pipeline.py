import json
from datetime import date

from minilakehouse import cli, lineage, runner
from minilakehouse.warehouse import fingerprint

D1, D3, D4 = date(2026, 10, 27), date(2026, 10, 29), date(2026, 10, 30)
GOLD = ["gold_churn_by_month", "gold_customer_360", "gold_daily_active_accounts", "gold_mrr_by_month"]


def by_model(rep):
    return {r.model: r for r in rep.results}


def test_full_run_succeeds_and_logs(con, home, models):
    rep = runner.run(con, home, models)
    assert rep.status == "success", rep.to_text()
    assert len(rep.results) == 12 and all(r.status == "success" for r in rep.results)
    assert len(rep.tests) >= 40 and all(t.status == "pass" for t in rep.tests)
    logged = con.execute("SELECT count(*), count(DISTINCT model) FROM _run_log WHERE run_id = ?",
                         [rep.run_id]).fetchone()
    assert logged == (12, 12)
    assert con.execute("SELECT status FROM _runs WHERE run_id = ?", [rep.run_id]).fetchone()[0] == "success"


def test_bronze_has_load_metadata(con, home, models):
    runner.run(con, home, models, selection=["bronze_billing_events"])
    row = con.execute("SELECT count(*), count(_source_file), count(_loaded_at), count(DISTINCT _batch_id), "
                      "min(_partition_date), max(_partition_date) FROM bronze_billing_events").fetchone()
    assert row[0] == row[1] == row[2] and row[3] == 4
    assert (row[4], row[5]) == (D1, D4)
    assert con.execute("SELECT count(*) FROM _bronze_manifest").fetchone()[0] == 4


def test_rerun_same_day_changes_nothing(con, home, models):
    first = runner.run(con, home, models, through=D3)
    fps = {n: fingerprint(con, n) for n in models if not n.startswith("bronze_")}
    again = by_model(runner.run(con, home, models, through=D3))
    for name, r in again.items():
        assert r.rows_touched == 0, name
        assert r.data == "same", name
        assert r.rows == by_model(first)[name].rows
    assert fps == {n: fingerprint(con, n) for n in fps}


def test_new_day_touches_only_its_partition(con, home, models):
    runner.run(con, home, models, through=D3)
    before = dict(con.execute("SELECT event_date, count(*) FROM silver_product_events GROUP BY 1").fetchall())
    res = by_model(runner.run(con, home, models))
    assert res["bronze_product_events"].partitions == (D4.isoformat(),)
    assert res["silver_product_events"].partitions == (D4.isoformat(),)
    after = dict(con.execute("SELECT event_date, count(*) FROM silver_product_events GROUP BY 1").fetchall())
    assert {d: n for d, n in after.items() if d != D4} == before
    assert max(res["silver_billing_events"].partitions) == D4.isoformat()
    assert res["silver_customers"].rows_touched < res["silver_customers"].rows


def test_late_arriving_event_merges_into_its_event_date(con, home, models):
    runner.run(con, home, models)
    late_dir = home / "landing" / "billing" / "dt=2026-10-31"
    late_dir.mkdir(parents=True)
    cust = con.execute("SELECT customer_id, subscription_id FROM silver_subscriptions LIMIT 1").fetchone()
    event = {"event_id": "evt_late_1", "event_type": "invoice_paid", "event_ts": "2026-10-28T12:00:00",
             "customer_id": cust[0], "subscription_id": cust[1], "plan": "team", "amount_minor": 9900,
             "currency": "USD"}
    (late_dir / "events.jsonl").write_text(json.dumps(event) + "\n")
    res = by_model(runner.run(con, home, models, selection=["bronze_billing_events+"]))
    assert res["silver_billing_events"].partitions == ("2026-10-28",)
    assert res["silver_billing_events"].rows_touched == 1
    row = con.execute("SELECT event_date, arrived_date, amount_cents FROM silver_billing_events "
                      "WHERE event_id = 'evt_late_1'").fetchone()
    assert row == (date(2026, 10, 28), date(2026, 10, 31), 9900)
    # Delivering the same late file again is a no-op.
    again = by_model(runner.run(con, home, models, selection=["bronze_billing_events+"]))
    assert again["silver_billing_events"].rows_touched == 0


def test_duplicates_are_removed(con, home, models, landing_4days):
    runner.run(con, home, models)
    bronze_rows, bronze_ids = con.execute(
        "SELECT count(*), count(DISTINCT event_id) FROM bronze_billing_events").fetchone()
    assert bronze_rows > bronze_ids  # the generator does send duplicates
    assert con.execute("SELECT count(*) FROM silver_billing_events").fetchone()[0] == bronze_ids


def test_schema_drift_is_logged_and_conformed(con, home, models):
    rep = by_model(runner.run(con, home, models))
    assert "amount_minor" in rep["bronze_billing_events"].message
    drift = con.execute("SELECT column_name, change FROM _schema_changes").fetchall()
    assert ("amount_minor", "added") in drift
    # Bronze keeps both raw columns; silver reads whichever one each row has.
    split = con.execute("SELECT count(*) FILTER (WHERE amount_cents IS NOT NULL), "
                        "count(*) FILTER (WHERE amount_minor IS NOT NULL) FROM bronze_billing_events").fetchone()
    assert split[0] > 0 and split[1] > 0
    nulls = con.execute("SELECT count(*) FROM silver_billing_events WHERE amount_cents IS NULL "
                        "AND event_type <> 'subscription_canceled'").fetchone()[0]
    assert nulls == 0


def test_any_of_falls_back_before_drift(con, home, models):
    runner.run(con, home, models, through=D1, selection=["bronze_billing_events"])
    sql = runner.render(models["silver_billing_events"], con, variables={"as_of": "2026-10-27"})
    assert 'CAST("amount_cents" AS BIGINT)' in sql and "amount_minor" not in sql.split("SELECT", 1)[1]


def test_failing_gate_blocks_gold(con, home, models):
    runner.run(con, home, models, through=D3)
    gold_before = {n: fingerprint(con, n) for n in GOLD}
    bad = home / "landing" / "billing" / "dt=2026-10-30" / "events.jsonl"
    with bad.open("a") as fh:
        fh.write(json.dumps({"event_id": "evt_ghost", "event_type": "invoice_paid",
                             "event_ts": "2026-10-30T09:00:00", "customer_id": "C99999",
                             "subscription_id": "S99999", "plan": "team", "amount_minor": 9900,
                             "currency": "USD"}) + "\n")
    rep = runner.run(con, home, models)
    res = by_model(rep)
    assert rep.status == "failed"
    assert all(res[n].status == "blocked" for n in GOLD)
    assert any("C99999" in g and "relationships" in g for g in rep.gate)
    assert "GATE FAILED" in rep.to_text()
    assert {n: fingerprint(con, n) for n in GOLD} == gold_before
    assert con.execute("SELECT count(*) FROM _run_log WHERE run_id = ? AND status = 'blocked'",
                       [rep.run_id]).fetchone()[0] == 4


def test_model_error_skips_downstream(con, home, models, tmp_path):
    broken = dict(models)
    broken["silver_invoices"] = type(models["silver_invoices"])(
        name="silver_invoices", layer="silver", sql="SELECT nope FROM {{ ref('silver_billing_events') }}",
        deps=["silver_billing_events"])
    rep = runner.run(con, home, broken)
    res = by_model(rep)
    assert res["silver_invoices"].status == "error"
    assert res["gold_customer_360"].status == "blocked"
    assert rep.status == "failed"


def test_select_runs_model_and_downstream_only(con, home, models):
    runner.run(con, home, models)
    rep = runner.run(con, home, models, selection=["silver_invoices+"])
    assert [r.model for r in rep.results] == ["silver_invoices", "gold_customer_360"]


def test_backfill_rewrites_range_without_changing_data(con, home, models):
    runner.run(con, home, models)
    fps = {n: fingerprint(con, n) for n in models if not n.startswith("bronze_")}
    rep = runner.run(con, home, models, backfill=(date(2026, 10, 28), date(2026, 10, 29)), command="backfill")
    res = by_model(rep)
    assert rep.status == "success", rep.to_text()
    assert res["bronze_crm_customers"].partitions == ("2026-10-28", "2026-10-29")
    assert res["silver_product_events"].partitions == ("2026-10-28", "2026-10-29")
    assert fps == {n: fingerprint(con, n) for n in fps}


def test_corrected_redelivery_replaces_batch(con, home, models):
    runner.run(con, home, models)
    f = home / "landing" / "crm" / "dt=2026-10-30" / "customers.csv"
    lines = f.read_text().splitlines()
    f.write_text("\n".join(lines[:-1]) + "\n")  # drop one row
    res = by_model(runner.run(con, home, models, selection=["bronze_crm_customers"]))
    assert res["bronze_crm_customers"].rows_touched == len(lines) - 2
    files = con.execute("SELECT count(*) FROM _bronze_manifest WHERE source = 'bronze_crm_customers'").fetchone()
    assert files[0] == 4


def test_test_command_checks_current_tables(con, home, models):
    rep = runner.test_only(con, models)
    assert rep.status == "failed"  # nothing built yet
    runner.run(con, home, models)
    assert runner.test_only(con, models).status == "success"


def test_lineage_export(con, home, models, tmp_path):
    runner.run(con, home, models)
    out = lineage.export(con, models, tmp_path / "docs")
    mmd = (tmp_path / "docs" / "lineage.mmd").read_text()
    assert mmd.startswith("flowchart LR")
    assert "silver_billing_events --> silver_invoices" in mmd and "src_billing --> bronze_billing_events" in mmd
    page = (tmp_path / "docs" / "lineage.html").read_text()
    assert "gold_mrr_by_month" in page and "amount_minor" in page and "<title>Lakehouse Lineage</title>" in page
    assert out["models"] == 12


def test_cli_end_to_end(tmp_path, capsys):
    h = str(tmp_path / "lh")
    assert cli.main(["--home", h, "generate", "--days", "3"]) == 0
    assert cli.main(["--home", h, "run"]) == 0
    assert cli.main(["--home", h, "run", "--select", "silver_invoices+"]) == 0
    assert cli.main(["--home", h, "backfill", "--from", "2026-10-27", "--to", "2026-10-28"]) == 0
    assert cli.main(["--home", h, "test"]) == 0
    assert cli.main(["--home", h, "lineage", "--out", str(tmp_path / "docs")]) == 0
    out = capsys.readouterr().out
    assert "status=success" in out and (tmp_path / "docs" / "lineage.html").exists()


def test_cli_corrupt_day_exits_nonzero(tmp_path, capsys):
    h = str(tmp_path / "lh")
    cli.main(["--home", h, "generate", "--days", "3", "--corrupt-last-day"])
    assert cli.main(["--home", h, "run"]) == 1
    assert "invoice_refunded_v2" in capsys.readouterr().out
