with source as (

    select * from {{ source('olist', 'olist_sellers_dataset') }}

),

renamed as (

    select
        seller_id,
        seller_zip_code_prefix,
        trim(lower(seller_city))                            as seller_city,
        upper(seller_state)                                 as seller_state

    from source

)

select * from renamed
