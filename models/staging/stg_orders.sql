WITH FINAL AS (
    SELECT
        MD5(CAST(id AS STRING)) AS order_id,
        id AS source_order_id,
        inventory_id AS source_inventory_id,
        customer_id AS source_customer_id,
        price,
        status,
        order_date
    FROM
        {{ ref('orders') }}
)
SELECT
    *
FROM
    FINAL
