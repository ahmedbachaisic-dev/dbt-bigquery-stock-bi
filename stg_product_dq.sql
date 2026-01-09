SELECT *
FROM `my-project-72993-480408.dataset3.stg_product`
WHERE product_id IS NULL OR TRIM(product_id)=''
    OR product_name IS NULL OR TRIM(product_name)=''
    OR category IS NULL OR TRIM(category)=''
    OR supplier_id IS NULL --OR TRIM(supplier_id)=''
    OR standard_cost IS NULL
    OR price IS  NULL
    OR reorder_level IS NULL
    OR target_level IS NULL
    OR quantity_per_unit IS NULL 
    OR discontinued IS NULL
    OR minimum_reorder_quantity IS NULL
Limit 100