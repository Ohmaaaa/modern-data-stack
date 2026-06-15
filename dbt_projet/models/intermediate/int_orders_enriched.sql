{{ config(materialized='view') }}

SELECT
    o.order_id,
    o.order_date,
    o.status,
    c.customer_id,
    c.first_name || ' ' || c.last_name   AS customer_name,
    c.country,
    oi.item_id,
    p.product_id,
    p.product_name,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.line_total
FROM {{ ref('stg_orders') }}      o
JOIN {{ ref('stg_order_items') }} oi ON o.order_id   = oi.order_id
JOIN {{ ref('stg_customers') }}   c  ON o.customer_id = c.customer_id
JOIN {{ ref('stg_products') }}    p  ON oi.product_id = p.product_id
