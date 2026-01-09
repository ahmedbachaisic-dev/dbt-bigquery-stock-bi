{{ 
    config(
        materialized='table',
        schema='staging'
    ) 
}}

WITH source AS (
    SELECT *
    FROM {{ ref('order_details') }}
),

renamed AS (
    SELECT
        CAST(id AS INT64)AS order_detail_id,
        order_id,
        SAFE_CAST(product_id AS INT64) AS product_id,
        COALESCE(quantity, 0) AS quantity,
        COALESCE(SAFE_CAST(unit_price AS NUMERIC), 0) AS unit_price,
        COALESCE(SAFE_CAST(discount AS NUMERIC), 0) AS discount,
        status_id,
        COALESCE(PARSE_DATE('%m/%d/%Y', CAST(date_allocated AS STRING)), CURRENT_DATE()) AS date_allocated,
        COALESCE(SAFE_CAST(purchase_order_id AS INT64), 0) AS purchase_order_id,
        COALESCE(SAFE_CAST(inventory_id AS INT64), 0) AS inventory_id
    FROM source
)

SELECT *
FROM renamed
WHERE order_detail_id IS NOT NULL
