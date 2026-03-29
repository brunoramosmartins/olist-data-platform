with source as (

    select * from {{ source('olist', 'olist_order_reviews_dataset') }}

),

renamed as (

    select
        review_id,
        order_id,
        cast(review_score as int)                           as review_score,
        review_comment_title,
        review_comment_message,
        cast(review_creation_date as timestamp)             as review_created_at,
        cast(review_answer_timestamp as timestamp)          as review_answered_at

    from source

)

select * from renamed
