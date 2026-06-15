{{ config(materialized='view') }}

SELECT
    CAST(item_id    AS INTEGER)          AS item_id,
    CAST(order_id   AS INTEGER)          AS order_id,
    CAST(product_id AS INTEGER)          AS product_id,
    CAST(quantity   AS INTEGER)          AS quantity,
    CAST(unit_price AS DECIMAL(10,2))    AS unit_price,
    CAST(quantity AS INTEGER) * CAST(unit_price AS DECIMAL(10,2)) AS line_total
FROM {{ source('ingested', 'order_items') }}
WHERE item_id IS NOT NULL
