/* config
materialized = "table"

[[tests]]
type = "unique"
column = "month"

[[tests]]
type = "not_null"
columns = ["month", "mrr_usd", "active_subscriptions"]

[[tests]]
type = "row_count_anomaly"
max_drop_pct = 20
*/
-- MRR at the end of each month (or at as_of for the current month), with the
-- new and churned MRR that moved it.
WITH months AS (
    SELECT CAST(m AS DATE) AS month
    FROM generate_series(
        (SELECT date_trunc('month', min(started_at)) FROM {{ ref('silver_subscriptions') }}),
        CAST(date_trunc('month', DATE '{{ var('as_of') }}') AS TIMESTAMP),
        INTERVAL 1 MONTH) AS g(m)
),
bounds AS (
    SELECT month,
           CAST(least(month + INTERVAL 1 MONTH, DATE '{{ var('as_of') }}' + INTERVAL 1 DAY) AS TIMESTAMP) AS cutoff
    FROM months
)
SELECT
    b.month,
    count(s.subscription_id) FILTER (
        WHERE s.started_at < b.cutoff AND (s.ended_at IS NULL OR s.ended_at >= b.cutoff))           AS active_subscriptions,
    coalesce(sum(s.mrr_usd) FILTER (
        WHERE s.started_at < b.cutoff AND (s.ended_at IS NULL OR s.ended_at >= b.cutoff)), 0)       AS mrr_usd,
    coalesce(sum(s.mrr_usd) FILTER (WHERE s.started_at >= b.month AND s.started_at < b.cutoff), 0) AS new_mrr_usd,
    coalesce(sum(s.mrr_usd) FILTER (WHERE s.ended_at >= b.month AND s.ended_at < b.cutoff), 0)     AS churned_mrr_usd
FROM bounds b
CROSS JOIN {{ ref('silver_subscriptions') }} s
GROUP BY b.month
ORDER BY b.month
