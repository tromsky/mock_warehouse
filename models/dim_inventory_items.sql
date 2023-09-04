WITH FINAL AS (
    SELECT
        SOURCE_COMPONENT_INVENTORY_ITEM_ID,
        COMPONENT_INVENTORY_ITEM_CODE,
        SELLABLE_INVENTORY_ITEM_TYPE,
        SALES_FRACTION,
        MD5(
            CONCAT(
                COMPONENT_INVENTORY_ITEM_ID,
                SELLABLE_INVENTORY_ITEM_ID
            )
        ) AS INVENTORY_ITEM_ID
    FROM
        {{ ref('stg_component_inventory_items') }}
)

SELECT *
FROM
    FINAL
