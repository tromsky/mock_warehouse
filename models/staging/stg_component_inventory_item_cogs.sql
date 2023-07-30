WITH FINAL AS (
    SELECT
        MD5(CAST(id AS STRING)) AS component_item_cogs_id,
        id AS source_component_item_cogs_id,
        inventory_id AS source_component_inventory_item_id,
        MD5(CAST(id AS STRING)) AS component_inventory_item_id,
        actual_cost
    FROM
        {{ ref('cogs') }}
)
SELECT
    *
FROM
    FINAL
