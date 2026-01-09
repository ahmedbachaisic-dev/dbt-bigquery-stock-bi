{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    id AS purchase_order_id,
    supplier_id,
    created_by,
    -- Conversion correcte de submitted_date
    CASE 
        WHEN submitted_date IS NULL OR TRIM(submitted_date) = '' THEN NULL
        WHEN submitted_date LIKE '%/%' THEN 
            SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_EXTRACT(submitted_date, r'^(\d{1,2}/\d{1,2}/\d{4})'))
        ELSE SAFE.PARSE_DATE('%Y-%m-%d', submitted_date)
    END AS submitted_date,
    -- Conversion correcte de creation_date
    CASE 
        WHEN creation_date IS NULL OR TRIM(creation_date) = '' THEN NULL
        WHEN creation_date LIKE '%/%' THEN 
            SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_EXTRACT(creation_date, r'^(\d{1,2}/\d{1,2}/\d{4})'))
        ELSE SAFE.PARSE_DATE('%Y-%m-%d', creation_date)
    END AS creation_date,
    status_id,
    COALESCE(payment_method, 'UNKNOWN') AS payment_method,
    COALESCE(notes, 'UNKNOWN') AS notes,
    COALESCE(approved_by, 0) AS approved_by,
    -- Conversion correcte de approved_date
    CASE 
        WHEN approved_date IS NULL OR TRIM(approved_date) = '' OR approved_date = 'UNKNOWN' THEN NULL
        WHEN approved_date LIKE '%/%' THEN 
            SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_EXTRACT(approved_date, r'^(\d{1,2}/\d{1,2}/\d{4})'))
        ELSE SAFE.PARSE_DATE('%Y-%m-%d', approved_date)
    END AS approved_date,
    COALESCE(submitted_by, 0) AS submitted_by,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ ref('purchase_orders') }}
WHERE id IS NOT NULL