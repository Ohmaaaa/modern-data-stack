{{ config(materialized='view') }}

SELECT
    CAST(product_id AS INTEGER)         AS product_id,
    TRIM(name)                          AS product_name,
    TRIM(category)                      AS category,
    CAST(unit_price AS DECIMAL(10,2))   AS unit_price,
    CAST(stock AS INTEGER)              AS stock
FROM {{ source('ingested', 'products') }}
WHERE product_id IS NOT NULL
