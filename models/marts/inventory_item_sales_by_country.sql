with sales as (

    select inventory_item_id, country_id, price
    from {{ ref('fct_sales') }}

),
countries as (

    select country_id, iso_code, name as country_name
    from {{ ref('dim_countries') }}

),
inventory_items as (

    select inventory_item_id, inventory_parent_code, inventory_child_code
    from {{ ref('dim_inventory_items') }}

),
final as (

    select

        countries.country_name,
        countries.iso_code,
        inventory_items.inventory_parent_code,
        sum(sales.price) as sales

    from sales
    join countries on sales.country_id = countries.country_id
    join inventory_items on sales.inventory_item_id = inventory_items.inventory_item_id

    group by 1, 2, 3
)

select * from final