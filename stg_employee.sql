select
    cast(id as INT64) as employee_id,
    trim(company) as company,
    trim(last_name) as last_name,
    trim(first_name) as first_name,
    trim(email_address) as email_address,
    trim(job_title) as job_title,
    trim(address) as address,
    trim(city) as city,
    trim(state_province) as state_province,
    zip_postal_code as code_postal,
    trim(country_region) as country_region,
    trim(web_page) as web_page,
    COALESCE(notes, 'Inconnu') AS notes
    


   
from {{ ref('employees') }}
