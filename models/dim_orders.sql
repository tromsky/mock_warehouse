with final as (

    select order_id, source_order_id, status
    from {{ ref('stg_orders') }}

)

select * from final