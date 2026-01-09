{{ config(materialized='table') }}

WITH pod AS (
    SELECT
        purchase_order_detail_id,
        purchase_order_id,
        product_id,
        quantity,
        unit_cost,
        DATE(date_received) AS date_received,
        posted_to_inventory,
        COALESCE(inventory_id, 0) AS inventory_id
    FROM {{ ref('stg_purchase_order_details') }}
),

po AS (
    SELECT
        purchase_order_id,
        supplier_id,
        created_by,
        DATE(creation_date) AS creation_date,
        status_id,
        COALESCE(payment_method, 'unknown') AS payment_method,
        COALESCE(notes, 'unknown') AS notes
    FROM {{ ref('stg_purchase_orders') }}
),

fact_with_keys AS (
    SELECT
        ---------------------------------------------------
        -- FACT KEY
        ---------------------------------------------------
        pod.purchase_order_detail_id AS fact_achat_id,

        ---------------------------------------------------
        -- DIMENSIONS KEYS
        ---------------------------------------------------
        dp.id_product   AS id_product,
        ds.id_supplier  AS id_supplier,
        de.id_employee  AS id_employee,

        ---------------------------------------------------
        -- DATE KEY (UNE SEULE RELATION AVEC dim_date)
        ---------------------------------------------------
        dd.id_date AS id_date,   

        ---------------------------------------------------
        -- MEASURES
        ---------------------------------------------------
        pod.quantity,
        pod.unit_cost,
        pod.quantity * pod.unit_cost AS total_cost,

        ---------------------------------------------------
        -- ATTRIBUTES
        ---------------------------------------------------
        po.status_id,
        po.payment_method,
        po.notes,
        po.creation_date,
        pod.date_received,
        pod.posted_to_inventory,
        pod.inventory_id

    FROM pod
    LEFT JOIN po
        ON pod.purchase_order_id = po.purchase_order_id

    LEFT JOIN {{ ref('dim_product') }} dp
        ON pod.product_id = dp.product_bk

    LEFT JOIN {{ ref('dim_supplier') }} ds
        ON po.supplier_id = ds.supplier_bk

    LEFT JOIN {{ ref('dim_employee') }} de
        ON po.created_by = de.employee_bk

    -- LIAISON DATE UNIQUE 
    LEFT JOIN {{ ref('dim_date') }} dd
        ON po.creation_date = dd.full_date
)

SELECT *
FROM fact_with_keys