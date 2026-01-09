SELECT *
FROM `my-project-72993-480408.dataset3.stg_customer`
WHERE customer_id IS NULL
   OR last_name IS NULL OR TRIM(last_name) = ''
   OR first_name IS NULL OR TRIM(first_name) = ''
   OR job_title IS NULL OR TRIM(job_title) = ''
   OR address IS NULL OR TRIM(address) = ''
   OR city IS NULL OR TRIM(city) = ''
   OR state_province IS NULL OR TRIM(state_province) = ''
   OR code_postal IS NULL 
   OR country_region IS NULL OR TRIM(country_region) = ''
LIMIT 100

