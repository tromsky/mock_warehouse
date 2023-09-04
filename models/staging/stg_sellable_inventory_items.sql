with sellable_inventory_items as (

    select
        id as source_sellable_inventory_item_id,
        code as sellable_inventory_item_code,
        md5(concat(cast(id as string))) as sellable_inventory_item_id
    from {{ ref('inventory') }}

),

inventory_item_parent_child as (

    select * from {{ ref('inventory_parent_child') }}

),

final as (

    select
        sellable_inventory_items.sellable_inventory_item_id,
        sellable_inventory_items.source_sellable_inventory_item_id,
        sellable_inventory_items.sellable_inventory_item_code,
        case
            when (inventory_item_parent_child.child_id is null)
                then (cast('standalone' as string))
            else (cast('combo' as string))
        end as sellable_inventory_item_type
    from sellable_inventory_items
    left join inventory_item_parent_child
        on
            sellable_inventory_items.source_sellable_inventory_item_id
            = inventory_item_parent_child.parent_id

)

select * from final
