{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        od.order_detail_id,
        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        DATE(o.order_date) AS order_date,
        o.shipped_date,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM {{ ref('stg_order') }} o
    LEFT JOIN {{ ref('stg_order_details') }} od
        ON od.order_id = o.order_id
    WHERE od.order_id IS NOT NULL
),

unique_source AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY order_detail_id
            ORDER BY insertion_timestamp DESC
        ) AS row_number
    FROM source
),

fact_with_keys AS (
    SELECT
        us.order_detail_id,
        dp.id_product AS id_product,                       
        dc.id_customer AS id_customer,                        
        de.id_employee AS id_employee,                        
        us.shipper_id,

        dd.id_date, 
        us.quantity,
        us.unit_price,
        us.discount,
        SAFE_CAST(us.quantity * us.unit_price * (1 - us.discount) AS NUMERIC) AS total_amount,
        us.status_id,
        us.date_allocated,
        us.purchase_order_id,
        us.inventory_id,
        us.shipped_date,
        us.insertion_timestamp,
        us.row_number
    FROM unique_source us
    LEFT JOIN {{ ref('dim_product') }} dp 
        ON us.product_id = dp.product_bk
    LEFT JOIN {{ ref('dim_customer') }} dc
        ON us.customer_id = dc.customer_bk
    LEFT JOIN {{ ref('dim_employee') }} de
        ON us.employee_id = de.employee_bk
    LEFT JOIN {{ ref('dim_date') }} dd
        ON us.order_date = dd.full_date
)

SELECT *
FROM fact_with_keys
WHERE row_number = 1