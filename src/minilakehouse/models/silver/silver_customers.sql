/* config
materialized = "incremental"
unique_key = "customer_id"
on_conflict = "update_if_newer:updated_at"

[[tests]]
type = "not_null"
columns = ["customer_id", "company_name", "created_at"]

[[tests]]
type = "unique"
column = "customer_id"

[[tests]]
type = "accepted_values"
column = "segment"
values = ["smb", "mid_market", "enterprise"]

[[tests]]
type = "row_count_anomaly"
max_drop_pct = 5
*/
-- Latest version of each customer from the daily CRM snapshots.
SELECT
    customer_id,
    trim(company_name)              AS company_name,
    upper(country)                  AS country,
    lower(segment)                  AS segment,
    CAST(created_at AS TIMESTAMP)   AS created_at,
    CAST(updated_at AS TIMESTAMP)   AS updated_at,
    _batch_id
FROM {{ ref('bronze_crm_customers') }}
WHERE {{ incremental_where }}
QUALIFY row_number() OVER (
    PARTITION BY customer_id ORDER BY CAST(updated_at AS TIMESTAMP) DESC, _partition_date, _batch_seq) = 1
