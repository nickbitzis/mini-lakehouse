from datetime import date

import pytest

from minilakehouse.quality import expand, run_test


@pytest.fixture
def db(mem):
    mem.execute("CREATE TABLE parent (id VARCHAR)")
    mem.execute("INSERT INTO parent VALUES ('a'), ('b')")
    mem.execute("CREATE TABLE child (id VARCHAR, parent_id VARCHAR, kind VARCHAR, amount INT, ts TIMESTAMP)")
    mem.execute("""INSERT INTO child VALUES
        ('1', 'a', 'paid', 10, '2026-11-01 10:00'),
        ('2', 'b', 'paid', NULL, '2026-11-01 11:00'),
        ('2', 'z', 'refund', NULL, '2026-10-30 09:00'),
        ('3', NULL, 'weird', 5, '2026-11-02 08:00')""")
    return mem


def test_not_null(db):
    assert run_test(db, "child", {"type": "not_null", "column": "id"}).status == "pass"
    r = run_test(db, "child", {"type": "not_null", "column": "amount"})
    assert r.status == "fail" and r.failures == 2


def test_not_null_with_where(db):
    r = run_test(db, "child", {"type": "not_null", "column": "amount", "where": "kind <> 'refund'"})
    assert r.failures == 1


def test_unique(db):
    r = run_test(db, "child", {"type": "unique", "column": "id"})
    assert r.status == "fail" and r.failures == 1 and "duplicated" in r.message
    assert run_test(db, "parent", {"type": "unique", "column": "id"}).status == "pass"


def test_accepted_values(db):
    r = run_test(db, "child", {"type": "accepted_values", "column": "kind", "values": ["paid", "refund"]})
    assert r.failures == 1 and "weird" in r.message


def test_relationships(db):
    r = run_test(db, "child", {"type": "relationships", "column": "parent_id", "to": "parent", "field": "id"})
    assert r.failures == 1 and "z" in r.message
    assert r.test == "relationships(parent_id -> parent.id)"


def test_freshness_pass_and_fail(db):
    spec = {"type": "freshness", "column": "ts", "max_age_hours": 48}
    assert run_test(db, "child", spec, as_of=date(2026, 11, 2)).status == "pass"
    r = run_test(db, "child", spec, as_of=date(2026, 11, 5))
    assert r.status == "fail" and "old" in r.message


def test_freshness_on_empty_table(mem):
    mem.execute("CREATE TABLE e (ts TIMESTAMP)")
    assert run_test(mem, "e", {"type": "freshness", "column": "ts", "max_age_hours": 1}).status == "fail"


def test_row_count_anomaly_vs_previous_run(db):
    spec = {"type": "row_count_anomaly", "max_drop_pct": 50, "max_growth_pct": 100}
    assert run_test(db, "child", spec, run_id="now").status == "pass"  # no history yet
    db.execute("INSERT INTO _run_log (run_id, model, started_at, rows, status) "
               "VALUES ('old', 'child', '2026-11-01', 10, 'success')")
    r = run_test(db, "child", spec, run_id="now")
    assert r.status == "fail" and "dropped 60%" in r.message
    db.execute("UPDATE _run_log SET rows = 1")
    assert "grew" in run_test(db, "child", spec, run_id="now").message
    db.execute("UPDATE _run_log SET rows = 4")
    assert run_test(db, "child", spec, run_id="now").status == "pass"


def test_warn_severity_does_not_block(db):
    r = run_test(db, "child", {"type": "unique", "column": "id", "severity": "warn"})
    assert r.status == "warn" and not r.blocking


def test_unknown_test_and_bad_column_are_errors(db):
    assert run_test(db, "child", {"type": "is_purple", "column": "id"}).status == "error"
    r = run_test(db, "child", {"type": "not_null", "column": "nope"})
    assert r.status == "error" and r.blocking


def test_expand_not_null_columns():
    specs = expand({"type": "not_null", "columns": ["a", "b"], "severity": "warn"})
    assert specs == [{"type": "not_null", "column": "a", "severity": "warn"},
                     {"type": "not_null", "column": "b", "severity": "warn"}]
