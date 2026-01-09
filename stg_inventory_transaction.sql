{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    id AS inventory_transaction_id,
    transaction_type,
    
    -- transaction_created_date
    CASE 
        WHEN transaction_created_date IS NULL OR TRIM(transaction_created_date) = '' THEN NULL
        WHEN transaction_created_date LIKE '%/%' THEN 
            SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', transaction_created_date)
        ELSE SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', transaction_created_date)
    END AS transaction_created_date,
    
    -- transaction_modified_date
    CASE 
        WHEN transaction_modified_date IS NULL OR TRIM(transaction_modified_date) = '' THEN NULL
        WHEN transaction_modified_date LIKE '%/%' THEN 
            SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', transaction_modified_date)
        ELSE SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', transaction_modified_date)
    END AS transaction_modified_date,
    
    product_id,
    quantity,
    
    -- purchase_order_id (peut être NULL) - déjà INT64 dans la source
    purchase_order_id,
    
    -- customer_order_id (peut être NULL) - déjà INT64 dans la source  
    customer_order_id,
    
    -- comments (peut être NULL)
    CASE 
        WHEN comments IS NULL OR TRIM(comments) = '' THEN 'NO_COMMENT'
        ELSE comments
    END AS comments,
    
    CURRENT_TIMESTAMP() AS insertion_timestamp

FROM {{ ref('inventory_transactions') }}
WHERE id IS NOT NULL