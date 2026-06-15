{{ config(materialized='view') }}

SELECT
    CAST(order_id      AS INTEGER)  AS order_id,
    CAST(customer_id   AS INTEGER)  AS customer_id,
    CAST(order_date    AS DATE)     AS order_date,
    TRIM(status)                    AS status,
    CAST(total_amount  AS DECIMAL(10,2)) AS total_amount
FROM {{ source('ingested', 'orders') }}
WHERE order_id IS NOT NULL
