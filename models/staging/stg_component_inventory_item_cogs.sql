WITH FINAL AS (
    SELECT
        ID AS SOURCE_COMPONENT_ITEM_COGS_ID,
        INVENTORY_ID AS SOURCE_COMPONENT_INVENTORY_ITEM_ID,
        ACTUAL_COST,
        -- TODO: Finish shaping this
        -- why is ID being used as two different MD5 ids?
        MD5(CAST(ID AS STRING)) AS COMPONENT_ITEM_COGS_ID,
        MD5(CAST(ID AS STRING)) AS COMPONENT_INVENTORY_ITEM_ID
    FROM
        {{ ref('cogs') }}
)

SELECT *
FROM
    FINAL
