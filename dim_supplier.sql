{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT 
        CAST(s.supplier_id AS INT64) AS supplier_bk,      -- Business Key
        s.company,
        s.last_name,
        s.first_name,
        s.job_title,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM {{ ref('stg_supplier') }} s
),

-- Déduplication : un seul enregistrement par fournisseur
unique_source AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY supplier_bk ORDER BY insertion_timestamp DESC) AS row_number
    FROM source
)

-- Sélection finale + ajout de la clé technique
SELECT
    {{ dbt_utils.generate_surrogate_key(['supplier_bk']) }} AS id_supplier,   -- 🔑 Clé technique stable
    supplier_bk,                                                             -- Business Key d'origine
    company,
    last_name,
    first_name,
    job_title,
    insertion_timestamp
FROM unique_source
WHERE row_number = 1