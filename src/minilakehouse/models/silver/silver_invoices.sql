/* config
materialized = "table"

[[tests]]
type = "unique"
column = "invoice_id"

[[tests]]
type = "not_null"
columns = ["invoice_id", "customer_id", "amount_usd"]
*/
SELECT
    event_id                AS invoice_id,
    customer_id,
    subscription_id,
    plan,
    event_ts                AS paid_at,
    event_date              AS paid_date,
    CAST(amount_cents / 100 AS DECIMAL(12, 2)) AS amount_usd
FROM {{ ref('silver_billing_events') }}
WHERE event_type = 'invoice_paid'
