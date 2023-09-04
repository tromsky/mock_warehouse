WITH FINAL AS (
    SELECT
        DATES.FISCAL_PERIOD,
        DATES.FISCAL_QTR,
        DATES.FISCAL_YEAR,
        SUM(SALES.PRICE) AS SALES
    FROM
        {{ ref('fct_sales') }}
            AS SALES
    INNER JOIN
        {{ ref('dim_dates') }}
            AS DATES
        ON SALES.DATE_ID = DATES.DATE_ID
    GROUP BY
        1,
        2,
        3
    ORDER BY
        1,
        2,
        3
)

SELECT *
FROM
    FINAL
