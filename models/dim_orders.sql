WITH FINAL AS (
    SELECT
        order_id,
        source_order_id,
        status
    FROM
        {{ ref('stg_orders') }}
)
SELECT
    *
FROM
    FINAL
