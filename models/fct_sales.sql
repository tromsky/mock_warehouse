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
FINAL AS (
    SELECT
        customer_id,
        order_id AS order_id_dd,
        inventory_item_id,
        date_id,
        price * sales_fraction AS price
    FROM
        customers
        JOIN orders
        ON customers.source_customer_id = orders.source_customer_id
        JOIN inventory_items
        ON orders.source_inventory_id = inventory_items.source_sellable_inventory_item_id
        JOIN dates
        ON dates.full_date = orders.order_date
)
SELECT
    *
FROM
    FINAL
