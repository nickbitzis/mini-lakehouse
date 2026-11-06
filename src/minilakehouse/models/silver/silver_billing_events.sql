/* config
materialized = "incremental"
unique_key = "event_id"
partition_by = "event_date"
on_conflict = "ignore"

[[tests]]
type = "not_null"
columns = ["event_id", "event_type", "event_ts", "customer_id", "subscription_id"]

[[tests]]
type = "not_null"
column = "amount_cents"
where = "event_type <> 'subscription_canceled'"

[[tests]]
type = "unique"
column = "event_id"

[[tests]]
type = "accepted_values"
column = "event_type"
values = ["subscription_created", "subscription_canceled", "invoice_paid"]

[[tests]]
type = "relationships"
column = "customer_id"
to = "silver_customers"
field = "customer_id"

[[tests]]
type = "freshness"
column = "event_ts"
max_age_hours = 48

[[tests]]
type = "row_count_anomaly"
max_drop_pct = 5
*/
-- Typed, deduplicated billing events. Late events land in the partition of
-- their event date, not their arrival date. The source renamed amount_cents
-- to amount_minor partway through; any_of() reads whichever exists.
SELECT
    event_id,
    lower(event_type)                                           AS event_type,
    CAST(event_ts AS TIMESTAMP)                                 AS event_ts,
    CAST(CAST(event_ts AS TIMESTAMP) AS DATE)                   AS event_date,
    customer_id,
    subscription_id,
    plan,
    CAST({{ any_of('bronze_billing_events', 'amount_minor', 'amount_cents') }} AS BIGINT) AS amount_cents,
    upper(currency)                                             AS currency,
    _partition_date                                             AS arrived_date,
    _batch_id
FROM {{ ref('bronze_billing_events') }}
WHERE {{ incremental_where }}
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _partition_date, _batch_seq) = 1
