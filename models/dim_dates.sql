WITH FINAL AS (
    SELECT *
    FROM
        {{ ref('stg_dates') }}
)

SELECT *
FROM
    FINAL
