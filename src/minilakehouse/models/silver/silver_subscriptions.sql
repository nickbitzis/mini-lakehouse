/* config
materialized = "table"

[[tests]]
type = "unique"
column = "subscription_id"

[[tests]]
type = "accepted_values"
column = "plan"
values = ["starter", "team", "enterprise"]

[[tests]]
type = "not_null"
columns = ["subscription_id", "customer_id", "started_at", "mrr_usd"]
*/
-- One row per subscription: when it started, when (if) it ended, and its monthly price.
WITH created AS (
    SELECT subscription_id, customer_id, plan, event_ts AS started_at, amount_cents
    FROM {{ ref('silver_billing_events') }}
    WHERE event_type = 'subscription_created'
    QUALIFY row_number() OVER (PARTITION BY subscription_id ORDER BY event_ts) = 1
),
canceled AS (
    SELECT subscription_id, min(event_ts) AS ended_at
    FROM {{ ref('silver_billing_events') }}
    WHERE event_type = 'subscription_canceled'
    GROUP BY 1
)
SELECT
    c.subscription_id,
    c.customer_id,
    c.plan,
    c.started_at,
    x.ended_at,
    CAST(c.amount_cents / 100 AS DECIMAL(12, 2)) AS mrr_usd,
    x.ended_at IS NULL      AS is_active
FROM created c
LEFT JOIN canceled x USING (subscription_id)
