WITH customers AS (
    SELECT
        customer_id,
        source_customer_id
    FROM
        {{ ref('stg_customers') }}
),

orders AS (
    SELECT
        order_id,
        source_customer_id,
        source_inventory_id,
        source_order_id,
        price,
        order_date
    FROM
        {{ ref('stg_orders') }}
),

inventory_items AS (
    SELECT
        inventory_item_id,
        component_inventory_item_id,
        source_component_inventory_item_id,
        sellable_inventory_item_id,
        source_sellable_inventory_item_id,
        component_inventory_item_code,
        sales_fraction
    FROM
        {{ ref('stg_component_inventory_items') }}
),

dates AS (
    SELECT
        date_id,
        full_date
    FROM
        {{ ref('stg_dates') }}
),

final AS (
    SELECT
        customers.customer_id,
        orders.order_id AS order_id_dd,
        inventory_items.inventory_item_id,
        dates.date_id,
        orders.price * inventory_items.sales_fraction AS price
    FROM
        customers
    INNER JOIN orders
        ON customers.source_customer_id = orders.source_customer_id
    INNER JOIN inventory_items
        ON
            orders.source_inventory_id
            = inventory_items.source_sellable_inventory_item_id
    INNER JOIN dates
        ON orders.order_date = dates.full_date
)

SELECT *
FROM
    final
