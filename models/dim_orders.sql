WITH FINAL AS (
    SELECT
        ORDER_ID,
        SOURCE_ORDER_ID,
        STATUS
    FROM
        {{ ref('stg_orders') }}
)

SELECT *
FROM
    FINAL
