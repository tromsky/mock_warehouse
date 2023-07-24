with final as (

    select
        inventory_item_id,
        order_id,
        distributed_price as price
    from {{ ref('stg_inventory_item_orders') }} 

)

select * from final