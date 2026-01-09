select
    cast(id as INT64) as customer_id,
    trim(last_name) as last_name,
    trim(company) as company,
    trim(first_name) as first_name,
    trim(job_title) as job_title,
    trim(address) as address,
    trim(city) as city,
    trim(state_province) as state_province,
    zip_postal_code as code_postal,
    trim(country_region) as country_region,
    
from {{ ref('customer') }}
