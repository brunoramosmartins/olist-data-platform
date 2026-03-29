with source as (

    select * from {{ source('olist', 'olist_products_dataset') }}

),

renamed as (

    select
        product_id,
        coalesce(
            lower(trim(product_category_name)),
            'uncategorized'
        )                                                   as product_category_name,
        cast(product_name_lenght as int)                    as product_name_length,
        cast(product_description_lenght as int)             as product_description_length,
        cast(product_photos_qty as int)                     as product_photos_qty,
        cast(product_weight_g as double)                    as weight_g,
        cast(product_length_cm as double)                   as length_cm,
        cast(product_height_cm as double)                   as height_cm,
        cast(product_width_cm as double)                    as width_cm

    from source

)

select * from renamed
