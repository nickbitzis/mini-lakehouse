/* config
materialized = "table"

[[tests]]
type = "unique"
column = "event_date"

[[tests]]
type = "freshness"
column = "event_date"
max_age_hours = 48
*/
SELECT
    event_date,
    count(DISTINCT customer_id)  AS active_accounts,
    count(*)                     AS events
FROM {{ ref('silver_product_events') }}
GROUP BY 1
ORDER BY 1
