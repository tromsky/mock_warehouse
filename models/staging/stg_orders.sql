WITH final AS (
    SELECT
        id AS source_order_id,
        inventory_id AS source_inventory_id,
        customer_id AS source_customer_id,
        price,
        status,
        order_date,
        MD5(CAST(id AS STRING)) AS order_id
    FROM
        {{ ref('orders') }}
)

SELECT *
FROM
    final
