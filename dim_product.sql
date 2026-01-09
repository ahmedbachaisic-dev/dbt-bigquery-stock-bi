SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS id_product,
    product_id AS product_bk,
    product_name,
    category,
    quantity_per_unit,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    minimum_reorder_quantity,
    discontinued
FROM {{ ref('stg_product') }}