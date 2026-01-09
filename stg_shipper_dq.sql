SELECT *
FROM `my-project-72993-480408.dataset3.stg_shipper`
WHERE shipper_id IS NULL
   Or company IS NULL OR TRIM(company)=''
   OR address IS NULL OR TRIM(address) = ''
   OR city IS NULL OR TRIM(city) = ''
   OR state_province IS NULL OR TRIM(state_province) = ''
   OR code_postal IS NULL 
   OR country_region IS NULL OR TRIM(country_region) = ''
LIMIT 100
