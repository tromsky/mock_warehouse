WITH final AS (
    SELECT *
    FROM
        {{ ref('stg_dates') }}
)

SELECT *
FROM
    final
