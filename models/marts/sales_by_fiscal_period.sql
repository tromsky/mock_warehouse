WITH final AS (
    SELECT
        dates.fiscal_period,
        dates.fiscal_qtr,
        dates.fiscal_year,
        SUM(sales.price) AS sales
    FROM
        {{ ref('fct_sales') }}
            AS sales
    INNER JOIN
        {{ ref('dim_dates') }}
            AS dates
        ON sales.date_id = dates.date_id
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
    final
