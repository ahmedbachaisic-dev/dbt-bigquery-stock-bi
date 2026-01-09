{{ config(materialized='table') }}

WITH inventory_transactions AS (
    SELECT
        inventory_transaction_id,
        transaction_type,
        transaction_created_date,
        product_id,
        quantity,
        purchase_order_id,
        customer_order_id,
        comments
    FROM {{ ref('stg_inventory_transaction') }}
),

dim_p AS (
    SELECT product_bk, id_product AS product_key
    FROM {{ ref('dim_product') }}
),

dim_d AS (
    SELECT full_date, id_date
    FROM {{ ref('dim_date') }}
)

SELECT
    -- KEYS
    it.inventory_transaction_id AS fact_stock_id,
    dim_p.product_key,
    dim_d.id_date,                 -- 🔑 clé date pour Power BI

    -- MEASURES
    it.quantity,

    -- ATTRIBUTES
    it.transaction_type,
    it.comments,
    it.transaction_created_date,
    it.purchase_order_id,
    it.customer_order_id,

    -- CALCULATED
    CASE 
        WHEN it.transaction_type IN (1, 2, 3) THEN 'IN'
        WHEN it.transaction_type IN (4, 5) THEN 'OUT'
        ELSE 'OTHER'
    END AS movement_direction

FROM inventory_transactions it
LEFT JOIN dim_p
    ON it.product_id = dim_p.product_bk
LEFT JOIN dim_d
    ON DATE(it.transaction_created_date) = dim_d.full_date
