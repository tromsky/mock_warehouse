with customers as (

    select customer_id, source_customer_id, source_country_id
    from {{ ref('stg_customers') }}

),
countries as (

    select country_id, source_country_id
    from {{ ref('stg_countries') }}

),
orders as (

    select order_id, source_customer_id, source_inventory_id, source_order_id
    from {{ ref('stg_orders') }}

),
inventory_items as (

    select inventory_item_id, distributed_price, source_inventory_parent_id, source_order_id
    from {{ ref('stg_inventory_item_orders') }}

),
final as (

    select
        customer_id,
        country_id,
        order_id,
        inventory_item_id,
        distributed_price as price
    from customers
    join countries
        on customers.source_country_id = countries.source_country_id
    join orders
        on customers.source_customer_id = orders.source_customer_id
    join inventory_items
        on orders.source_inventory_id = inventory_items.source_inventory_parent_id
        and orders.source_order_id = inventory_items.source_order_id

)

select * from final