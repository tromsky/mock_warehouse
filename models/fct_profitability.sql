WITH inventory_items AS (
    SELECT *
    FROM
        {{ ref('stg_component_inventory_items') }}
),

orders AS (
    SELECT
        order_id,
        source_customer_id,
        source_inventory_id,
        source_order_id,
        price
    FROM
        {{ ref('stg_orders') }}
),

component_item_cogs AS (
    SELECT
        source_component_inventory_item_id,
        component_inventory_item_id,
        SUM(actual_cost) AS actual_cost
    FROM
        {{ ref('stg_component_inventory_item_cogs') }}
    GROUP BY
        1,
        2
),

final AS (
    SELECT
        inventory_items.inventory_item_id,
        component_item_cogs.actual_cost AS cogs,
        orders.price * inventory_items.sales_fraction AS sales,
        (
            orders.price * inventory_items.sales_fraction
        ) - component_item_cogs.actual_cost AS margin
    FROM
        inventory_items
    INNER JOIN orders
        ON
            inventory_items.source_sellable_inventory_item_id
            = orders.source_inventory_id
    INNER JOIN component_item_cogs
        ON
            inventory_items.source_component_inventory_item_id
            = component_item_cogs.source_component_inventory_item_id
            AND inventory_items.source_sellable_inventory_item_id
            = component_item_cogs.source_component_inventory_item_id
)

SELECT *
FROM
    final
