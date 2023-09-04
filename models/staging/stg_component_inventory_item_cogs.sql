WITH final AS (
    SELECT
        id AS source_component_item_cogs_id,
        inventory_id AS source_component_inventory_item_id,
        actual_cost,
        -- TODO: Finish shaping this
        -- why is ID being used as two different MD5 ids?
        MD5(CAST(id AS STRING)) AS component_item_cogs_id,
        MD5(CAST(id AS STRING)) AS component_inventory_item_id
    FROM
        {{ ref('cogs') }}
)

SELECT *
FROM
    final
