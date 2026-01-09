{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        CAST(c.customer_id AS INT64) AS customer_bk,       -- Business Key
        c.company,
        c.last_name,
        c.first_name,
        c.job_title,
        c.address,
        c.city,
        c.state_province,
        c.code_postal,
        c.country_region,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM {{ ref('stg_customer') }} c
),

-- Déduplication : un seul enregistrement par client
unique_source AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY customer_bk ORDER BY insertion_timestamp DESC) AS row_number
    FROM source
)

-- Sélection finale + ajout de la clé technique
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_bk']) }} AS id_customer,   -- 🔑 Clé technique stable
    customer_bk,                                                              -- Business Key d'origine
    company,
    last_name,
    first_name,
    job_title,
    address,
    city,
    state_province,
    code_postal,
    country_region,
    insertion_timestamp
FROM unique_source
WHERE row_number = 1
