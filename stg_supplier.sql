select
    cast(id as int) as supplier_id,
    trim(company) as company,
    trim(last_name) as last_name,
    trim(first_name) as first_name,
    trim(job_title) as job_title,

    


    
from {{ ref('suppliers') }}
