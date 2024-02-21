WITH final AS (

    SELECT
        inventory_parent.id AS source_inventory_parent_id,
        inventory_parent.code AS inventory_parent_code,
        inventory_child.id AS source_inventory_child_id,
        inventory_child.code AS inventory_child_code,
        md5(
            concat(
                cast(inventory_parent.id AS string),
                cast(
                    coalesce(inventory_child.id, inventory_parent.id) AS string
                )
            )
        ) AS inventory_item_id,
        coalesce(inventory_parent_child.sales_fraction, 1) AS sales_fraction
    FROM {{ ref('inventory') }} AS inventory_parent
    LEFT JOIN {{ ref('inventory_parent_child') }} AS inventory_parent_child
        ON inventory_parent.id = inventory_parent_child.parent_id
    LEFT JOIN {{ ref('inventory') }} AS inventory_child
        ON inventory_parent_child.child_id = inventory_child.id

)

SELECT * FROM final
