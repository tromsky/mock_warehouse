with inventory_items as (

    select * from {{ ref('stg_inventory_items') }}

),
orders as (

    select * from {{ ref('stg_orders') }}

),
final as (

    select 
        md5(concat(inventory_item_id, order_id)) as inventory_item_order_id,
        inventory_items.inventory_item_id,
        orders.order_id,
        orders.source_order_id,
        inventory_items.source_inventory_parent_id,
        inventory_items.source_inventory_child_id,
        inventory_items.inventory_parent_code,
        inventory_items.inventory_child_code,
        inventory_items.sales_fraction * orders.price as distributed_price
    from inventory_items
    join orders
        on inventory_items.source_inventory_parent_id = orders.source_inventory_id

)

select * from final