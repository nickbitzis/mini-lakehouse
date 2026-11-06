/* config
materialized = "incremental"
unique_key = "event_id"
partition_by = "event_date"
on_conflict = "ignore"

[[tests]]
type = "not_null"
columns = ["event_id", "customer_id", "event_ts"]

[[tests]]
type = "unique"
column = "event_id"

[[tests]]
type = "relationships"
column = "customer_id"
to = "silver_customers"
field = "customer_id"

[[tests]]
type = "freshness"
column = "event_ts"
max_age_hours = 48
*/
SELECT
    event_id,
    account_id                                  AS customer_id,
    event_name,
    CAST(event_ts AS TIMESTAMP)                 AS event_ts,
    CAST(CAST(event_ts AS TIMESTAMP) AS DATE)   AS event_date,
    _batch_id
FROM {{ ref('bronze_product_events') }}
WHERE {{ incremental_where }}
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _partition_date, _batch_seq) = 1
