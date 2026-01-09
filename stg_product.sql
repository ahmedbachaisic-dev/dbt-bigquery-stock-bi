{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    id AS product_id,
    TRIM(product_name) AS product_name,
    TRIM(category) AS category,
    TRIM(quantity_per_unit) AS quantity_per_unit,
    CAST(standard_cost AS NUMERIC) AS standard_cost,
    CAST(list_price AS NUMERIC) AS list_price,
    CAST(reorder_level AS INT64) AS reorder_level,
    CAST(target_level AS INT64) AS target_level,
    CAST(minimum_reorder_quantity AS INT64) AS minimum_reorder_quantity,
    discontinued
FROM {{ ref('products') }}
WHERE id IS NOT NULL