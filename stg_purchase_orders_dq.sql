{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    po.purchase_order_id,
    po.supplier_id,
    po.created_by,
    
    -- Tests généraux simples
    CASE WHEN supplier_id IS NULL THEN 'KO' ELSE 'OK' END AS supplier_id_present,
    CASE WHEN created_by IS NULL THEN 'KO' ELSE 'OK' END AS created_by_present,
    CASE WHEN status_id IS NULL THEN 'KO' ELSE 'OK' END AS status_present,

    CASE WHEN submitted_date IS NULL THEN 'WARNING' ELSE 'OK' END AS submitted_date_valid,
    CASE WHEN creation_date IS NULL THEN 'WARNING' ELSE 'OK' END AS creation_date_valid

FROM {{ ref('stg_purchase_orders') }} po