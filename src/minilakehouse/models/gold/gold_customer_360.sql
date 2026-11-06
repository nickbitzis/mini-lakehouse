/* config
materialized = "table"

[[tests]]
type = "unique"
column = "customer_id"

[[tests]]
type = "relationships"
column = "customer_id"
to = "silver_customers"
field = "customer_id"

[[tests]]
type = "accepted_values"
column = "status"
values = ["active", "churned", "never_subscribed"]
*/
-- One row per customer: profile, plan, revenue, and recent usage.
WITH subs AS (
    SELECT customer_id,
           sum(mrr_usd) FILTER (WHERE is_active)                AS mrr_usd,
           arg_max(plan, started_at) FILTER (WHERE is_active)   AS current_plan,
           count(*)                                             AS subscriptions,
           bool_or(is_active)                                   AS any_active
    FROM {{ ref('silver_subscriptions') }}
    GROUP BY 1
),
rev AS (
    SELECT customer_id, sum(amount_usd) AS lifetime_revenue_usd, count(*) AS invoices, max(paid_at) AS last_paid_at
    FROM {{ ref('silver_invoices') }}
    GROUP BY 1
),
usage AS (
    SELECT customer_id,
           max(event_ts) AS last_seen_at,
           count(*) FILTER (WHERE event_date > DATE '{{ var('as_of') }}' - INTERVAL 7 DAY) AS events_last_7d
    FROM {{ ref('silver_product_events') }}
    GROUP BY 1
)
SELECT
    c.customer_id,
    c.company_name,
    c.segment,
    c.country,
    c.created_at,
    CASE WHEN s.customer_id IS NULL THEN 'never_subscribed'
         WHEN s.any_active THEN 'active' ELSE 'churned' END        AS status,
    s.current_plan,
    coalesce(s.mrr_usd, 0)                                         AS mrr_usd,
    coalesce(r.lifetime_revenue_usd, 0)                            AS lifetime_revenue_usd,
    coalesce(r.invoices, 0)                                        AS invoices,
    r.last_paid_at,
    u.last_seen_at,
    coalesce(u.events_last_7d, 0)                                  AS events_last_7d
FROM {{ ref('silver_customers') }} c
LEFT JOIN subs s USING (customer_id)
LEFT JOIN rev r USING (customer_id)
LEFT JOIN usage u USING (customer_id)
