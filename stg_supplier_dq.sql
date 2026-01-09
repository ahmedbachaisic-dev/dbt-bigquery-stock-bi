SELECT *
FROM `my-project-72993-480408.dataset3.stg_supplier`
WHERE supplier_id IS NULL
   OR last_name IS NULL OR TRIM(last_name) = ''
   OR first_name IS NULL OR TRIM(first_name) = ''
   OR company IS NULL OR TRIM(company)=''
   OR job_title IS NULL OR TRIM(job_title) = ''
   
LIMIT 100
