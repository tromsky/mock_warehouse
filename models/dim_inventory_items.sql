WITH final AS (
    SELECT
        source_component_inventory_item_id,
        component_inventory_item_code,
        sellable_inventory_item_type,
        sales_fraction,
        MD5(
            CONCAT(
                component_inventory_item_id,
                sellable_inventory_item_id
            )
        ) AS inventory_item_id
    FROM
        {{ ref('stg_component_inventory_items') }}
)

SELECT *
FROM
    final
