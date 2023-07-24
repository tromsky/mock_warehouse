with final as (

    select
        md5(cast(id as string)) as order_id,
        id as source_order_id,
        inventory_id as source_inventory_id,
        customer_id as source_customer_id,
        price,
        status
    from {{ ref('orders') }}

)

select * from final