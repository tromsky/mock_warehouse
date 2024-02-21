WITH sellable_inventory_items AS (

    SELECT
        id AS source_sellable_inventory_item_id,
        code AS sellable_inventory_item_code,
        md5(concat(cast(id AS string))) AS sellable_inventory_item_id
    FROM {{ ref('inventory') }}

),

inventory_item_parent_child AS (

    SELECT * FROM {{ ref('inventory_parent_child') }}

),

final AS (

    SELECT
        sellable_inventory_items.sellable_inventory_item_id,
        sellable_inventory_items.source_sellable_inventory_item_id,
        sellable_inventory_items.sellable_inventory_item_code,
        CASE
            WHEN (inventory_item_parent_child.child_id IS null)
                THEN (cast('standalone' AS string))
            ELSE (cast('combo' AS string))
        END AS sellable_inventory_item_type
    FROM sellable_inventory_items
    LEFT JOIN inventory_item_parent_child
        ON
            sellable_inventory_items.source_sellable_inventory_item_id
            = inventory_item_parent_child.parent_id

)

SELECT * FROM final
