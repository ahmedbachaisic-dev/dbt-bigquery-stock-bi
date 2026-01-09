WITH source AS (
    SELECT
        CAST(e.employee_id AS INT64) AS employee_bk,  -- Forcer le type INT64
        e.company,
        e.last_name,
        e.first_name,
        e.email_address,
        e.job_title,
        e.address,
        e.city,
        e.state_province,
        e.code_postal,
        e.country_region,
        e.web_page,
        e.notes,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM {{ ref('stg_employee') }} e
),
unique_source AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY employee_bk ORDER BY insertion_timestamp DESC) AS row_number
    FROM source
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['employee_bk']) }} AS id_employee, 
    employee_bk,
    company,
    last_name,
    first_name,
    email_address,
    job_title,
    address,
    city,
    state_province,
    code_postal,
    country_region,
    web_page,
    notes,
    insertion_timestamp
FROM unique_source
WHERE row_number = 1
