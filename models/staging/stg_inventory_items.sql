with final as (

    select
        md5(concat(cast(inventory_parent.id as string), cast(coalesce(inventory_child.id, inventory_parent.id) as string))) as inventory_item_id,
        inventory_parent.id as source_inventory_parent_id,
        inventory_parent.code as inventory_parent_code,
        inventory_child.id as source_inventory_child_id,
        inventory_child.code as inventory_child_code,
        coalesce(inventory_parent_child.sales_fraction, 1) as sales_fraction
    from {{ ref('inventory') }} inventory_parent
    left join {{ ref('inventory_parent_child') }} inventory_parent_child
        on inventory_parent.id = inventory_parent_child.parent_id
    left join {{ ref('inventory') }} inventory_child
        on inventory_parent_child.child_id = inventory_child.id

)

select * from final