WITH sellable_inventory_items AS (
    SELECT
        *
    FROM
        {{ ref('stg_sellable_inventory_items') }}
),
inventory_item_parent_child AS (
    SELECT
        *
    FROM
        {{ ref('inventory_parent_child') }}
),
component_inventory_items AS (
    SELECT
        MD5(CONCAT(CAST(id AS STRING))) AS component_inventory_item_id,
        id AS source_component_inventory_item_id,
        code AS component_inventory_item_code
    FROM
        {{ ref('inventory') }}
),
inventory_items AS (
    SELECT
        COALESCE(
            component_inventory_items.component_inventory_item_id,
            sellable_inventory_items.sellable_inventory_item_id
        ) AS component_inventory_item_id,
        COALESCE(
            component_inventory_items.source_component_inventory_item_id,
            sellable_inventory_items.source_sellable_inventory_item_id
        ) AS source_component_inventory_item_id,
        sellable_inventory_items.sellable_inventory_item_id,
        sellable_inventory_items.source_sellable_inventory_item_id,
        COALESCE(
            component_inventory_items.component_inventory_item_code,
            sellable_inventory_items.sellable_inventory_item_code
        ) AS component_inventory_item_code,
        CASE
            WHEN (
                inventory_item_parent_child.child_id IS NULL
            ) THEN (CAST('standalone' AS STRING))
            ELSE (CAST('core' AS STRING))
        END AS sellable_inventory_item_type,
        COALESCE(
            inventory_item_parent_child.sales_fraction,
            1
        ) AS sales_fraction
    FROM
        sellable_inventory_items
        LEFT JOIN inventory_item_parent_child
        ON sellable_inventory_items.source_sellable_inventory_item_id = inventory_item_parent_child.parent_id
        LEFT JOIN component_inventory_items
        ON inventory_item_parent_child.child_id = component_inventory_items.source_component_inventory_item_id
),
FINAL AS (
    SELECT
        MD5(
            CONCAT(
                COALESCE(
                    component_inventory_item_id,
                    sellable_inventory_item_id
                ),
                sellable_inventory_item_id
            )
        ) AS inventory_item_id,
        COALESCE(
            component_inventory_item_id,
            sellable_inventory_item_id
        ) AS component_inventory_item_id,
        COALESCE(
            source_component_inventory_item_id,
            source_sellable_inventory_item_id
        ) AS source_component_inventory_item_id,
        sellable_inventory_item_id,
        source_sellable_inventory_item_id,
        COALESCE(
            component_inventory_item_code,
            sellable_inventory_item_code
        ) AS component_inventory_item_code,
        sellable_inventory_item_type,
        COALESCE(
            sales_fraction,
            1
        ) AS sales_fraction
    FROM
        sellable_inventory_items
        LEFT JOIN inventory_item_parent_child
        ON sellable_inventory_items.source_sellable_inventory_item_id = inventory_item_parent_child.parent_id
        LEFT JOIN component_inventory_items
        ON inventory_item_parent_child.child_id = component_inventory_items.source_component_inventory_item_id
)
SELECT
    *
FROM
    FINAL
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
