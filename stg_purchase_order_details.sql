{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    id AS purchase_order_detail_id,
    purchase_order_id,
    product_id,
    quantity,
    unit_cost,
    -- Conversion correcte de date_received
    CASE 
        WHEN date_received IS NULL OR TRIM(date_received) = '' THEN NULL
        WHEN date_received LIKE '%/%' THEN 
            SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_EXTRACT(date_received, r'^(\d{1,2}/\d{1,2}/\d{4})'))
        ELSE SAFE.PARSE_DATE('%Y-%m-%d', date_received)
    END AS date_received,
    posted_to_inventory,
    COALESCE(inventory_id, 0) AS inventory_id,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ ref('purchase_order_details') }}
WHERE id IS NOT NULL