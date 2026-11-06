"""Gold metrics on a tiny dataset checked by hand."""

import pytest

from minilakehouse.runner import build_model

AS_OF = {"as_of": "2026-03-03"}


@pytest.fixture
def subs(mem):
    # A: starter $29 from Jan 15, still active.
    # B: team $99 from Jan 20, canceled Feb 10.
    # C: enterprise $499 from Feb 5, still active.
    mem.execute("""CREATE TABLE silver_subscriptions AS SELECT * FROM (VALUES
        ('A', 'c1', 'starter',    TIMESTAMP '2026-01-15 10:00', NULL::TIMESTAMP,          29.0, true),
        ('B', 'c2', 'team',       TIMESTAMP '2026-01-20 10:00', TIMESTAMP '2026-02-10 08:00', 99.0, false),
        ('C', 'c3', 'enterprise', TIMESTAMP '2026-02-05 10:00', NULL::TIMESTAMP,          499.0, true)
    ) t(subscription_id, customer_id, plan, started_at, ended_at, mrr_usd, is_active)""")
    return mem


def test_mrr_by_month(subs, models):
    build_model(subs, models["gold_mrr_by_month"], variables=AS_OF)
    rows = subs.execute("SELECT strftime(month, '%Y-%m'), active_subscriptions, mrr_usd, new_mrr_usd, "
                        "churned_mrr_usd FROM gold_mrr_by_month ORDER BY month").fetchall()
    assert rows == [
        ("2026-01", 2, 128.0, 128.0, 0.0),
        ("2026-02", 2, 528.0, 499.0, 99.0),
        ("2026-03", 2, 528.0, 0.0, 0.0),
    ]


def test_mrr_moves_reconcile(subs, models):
    build_model(subs, models["gold_mrr_by_month"], variables=AS_OF)
    rows = subs.execute("SELECT mrr_usd, new_mrr_usd, churned_mrr_usd FROM gold_mrr_by_month "
                        "ORDER BY month").fetchall()
    prev = 0
    for mrr, new, churned in rows:
        assert prev + new - churned == mrr
        prev = mrr


def test_churn_by_month(subs, models):
    build_model(subs, models["gold_churn_by_month"], variables=AS_OF)
    rows = subs.execute("SELECT strftime(month, '%Y-%m'), subscriptions_at_start, churned, churned_mrr_usd, "
                        "logo_churn_rate FROM gold_churn_by_month ORDER BY month").fetchall()
    assert rows == [
        ("2026-01", 0, 0, 0.0, None),
        ("2026-02", 2, 1, 99.0, 0.5),
        ("2026-03", 2, 0, 0.0, 0.0),
    ]


def test_mrr_cutoff_respects_as_of(subs, models):
    # As of Feb 7, C (started Feb 5) counts and B (ended Feb 10) is still active.
    build_model(subs, models["gold_mrr_by_month"], variables={"as_of": "2026-02-07"})
    feb = subs.execute("SELECT active_subscriptions, mrr_usd FROM gold_mrr_by_month "
                       "WHERE month = DATE '2026-02-01'").fetchone()
    assert feb == (3, 627.0)
