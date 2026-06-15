{{ config(materialized='table', schema='analytics') }}

SELECT
    category,
    COUNT(DISTINCT order_id)    AS total_orders,
    SUM(quantity)               AS units_sold,
    SUM(line_total)             AS total_revenue,
    CURRENT_TIMESTAMP           AS refreshed_at
FROM {{ ref('int_orders_enriched') }}
WHERE status != 'cancelled'
GROUP BY category
ORDER BY total_revenue DESC
