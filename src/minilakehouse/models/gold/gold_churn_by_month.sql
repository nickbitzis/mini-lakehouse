/* config
materialized = "table"

[[tests]]
type = "unique"
column = "month"

[[tests]]
type = "not_null"
columns = ["month", "subscriptions_at_start", "churned"]
*/
-- Logo churn: of the subscriptions active when the month began, how many ended in it.
WITH months AS (
    SELECT CAST(m AS DATE) AS month
    FROM generate_series(
        (SELECT date_trunc('month', min(started_at)) FROM {{ ref('silver_subscriptions') }}),
        CAST(date_trunc('month', DATE '{{ var('as_of') }}') AS TIMESTAMP),
        INTERVAL 1 MONTH) AS g(m)
),
bounds AS (
    SELECT month,
           CAST(month AS TIMESTAMP) AS month_start,
           CAST(least(month + INTERVAL 1 MONTH, DATE '{{ var('as_of') }}' + INTERVAL 1 DAY) AS TIMESTAMP) AS cutoff
    FROM months
),
agg AS (
    SELECT
        b.month,
        count(*) FILTER (WHERE s.started_at < b.month_start
                           AND (s.ended_at IS NULL OR s.ended_at >= b.month_start))       AS subscriptions_at_start,
        count(*) FILTER (WHERE s.started_at < b.month_start
                           AND s.ended_at >= b.month_start AND s.ended_at < b.cutoff)    AS churned,
        coalesce(sum(s.mrr_usd) FILTER (WHERE s.started_at < b.month_start
                           AND s.ended_at >= b.month_start AND s.ended_at < b.cutoff), 0) AS churned_mrr_usd
    FROM bounds b
    CROSS JOIN {{ ref('silver_subscriptions') }} s
    GROUP BY b.month
)
SELECT
    month,
    subscriptions_at_start,
    churned,
    churned_mrr_usd,
    CASE WHEN subscriptions_at_start = 0 THEN NULL
         ELSE round(churned / subscriptions_at_start, 4) END AS logo_churn_rate
FROM agg
ORDER BY month
