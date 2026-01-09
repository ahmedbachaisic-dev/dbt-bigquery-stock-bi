{{ 
    config(
        materialized='view',
        schema='staging'
    ) 
}}

SELECT
    id AS order_id,
    employee_id,
    customer_id,

    -- dates
    PARSE_DATE('%m/%d/%Y', SPLIT(order_date, ' ')[0]) AS order_date,
    PARSE_DATE('%m/%d/%Y', SPLIT(shipped_date, ' ')[0]) AS shipped_date,

    shipper_id,

    -- text cleanup
    COALESCE(TRIM(CAST(ship_name AS STRING)), 'Unknown') AS ship_name,
    COALESCE(TRIM(CAST(ship_address AS STRING)), 'Unknown') AS ship_address,
    COALESCE(TRIM(CAST(ship_city AS STRING)), 'Unknown') AS ship_city,
    COALESCE(TRIM(CAST(ship_state_province AS STRING)), 'Unknown') AS ship_state_province,
    COALESCE(TRIM(CAST(ship_zip_postal_code AS STRING)), 'Unknown') AS ship_code_postal,
    COALESCE(TRIM(CAST(ship_country_region AS STRING)), 'Unknown') AS ship_country_region,

    -- numeric
    COALESCE(SAFE_CAST(shipping_fee AS NUMERIC), 0) AS shipping_fee,

    COALESCE(TRIM(CAST(payment_type AS STRING)), 'Unknown') AS payment_type,

    status_id

FROM {{ ref('orders') }}
WHERE id IS NOT NULL
