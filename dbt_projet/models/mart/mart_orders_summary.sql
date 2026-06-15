{{ config(materialized='table', schema='analytics') }}

SELECT
    status,
    COUNT(DISTINCT order_id)    AS nb_orders,
    SUM(line_total)             AS revenue,
    CURRENT_TIMESTAMP           AS refreshed_at
FROM {{ ref('int_orders_enriched') }}
GROUP BY status
ORDER BY nb_orders DESC
