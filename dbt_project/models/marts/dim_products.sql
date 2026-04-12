-- dim_products.sql
--
-- Product dimension. One row per product_id. Null or empty categories become 'uncategorized'.

with products as (

    select * from {{ ref('stg_products') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        coalesce(
            nullif(trim(product_category_name), ''),
            'uncategorized'
        ) as product_category,
        weight_g,
        length_cm,
        height_cm,
        width_cm
    from products

)

select * from final
