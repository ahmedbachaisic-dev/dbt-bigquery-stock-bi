select
    cast(id as INT64) as shipper_id,
    trim(company) as company,
    trim(address) as address,
    trim(city) as city,
    trim(state_province) as state_province,
    zip_postal_code as code_postal,
    trim(country_region) as country_region,
    

from {{ ref('shippers') }}
