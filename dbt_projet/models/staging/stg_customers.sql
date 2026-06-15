{{ config(materialized='view') }}

SELECT
    CAST(customer_id AS INTEGER)    AS customer_id,
    TRIM(first_name)                AS first_name,
    TRIM(last_name)                 AS last_name,
    LOWER(TRIM(email))              AS email,
    TRIM(country)                   AS country
FROM {{ source('ingested', 'customers') }}
WHERE customer_id IS NOT NULL
