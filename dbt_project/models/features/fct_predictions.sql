-- fct_predictions.sql
--
-- Bridge: batch ML predictions joined to the order fact for analysis.
-- Requires main.ml_predictions (python ml/predict.py).

with predictions as (

    select * from {{ source('ml_pipeline', 'ml_predictions') }}

),

orders as (

    select * from {{ ref('fact_orders') }}

),

final as (

    select
        p.order_id,
        p.predicted_probability,
        p.predicted_class,
        p.model_version,
        p.prediction_timestamp,
        o.customer_key,
        o.order_date,
        o.is_delayed as actual_is_delayed
    from predictions p
    left join orders o
        on p.order_id = o.order_id

)

select * from final
