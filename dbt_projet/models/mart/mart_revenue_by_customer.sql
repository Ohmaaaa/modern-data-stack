{{ config(materialized='table', schema='analytics') }}

SELECT
    customer_id,
    customer_name,
    country,
    COUNT(DISTINCT order_id)    AS total_orders,
    SUM(line_total)             AS total_revenue,
    AVG(line_total)             AS avg_order_value,
    CURRENT_TIMESTAMP           AS refreshed_at
FROM {{ ref('int_orders_enriched') }}
WHERE status != 'cancelled'
GROUP BY customer_id, customer_name, country
ORDER BY total_revenue DESC
