WITH inventory_items AS (

    SELECT * FROM {{ ref('stg_inventory_items') }}

),

orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

final AS (

    SELECT
        inventory_items.inventory_item_id,
        orders.order_id,
        orders.source_order_id,
        inventory_items.source_inventory_parent_id,
        inventory_items.source_inventory_child_id,
        inventory_items.inventory_parent_code,
        inventory_items.inventory_child_code,
        md5(concat(inventory_items.inventory_item_id, orders.order_id))
            AS inventory_item_order_id,
        inventory_items.sales_fraction * orders.price AS distributed_price
    FROM inventory_items
    INNER JOIN orders
        ON
            inventory_items.source_inventory_parent_id
            = orders.source_inventory_id

)

SELECT * FROM final
