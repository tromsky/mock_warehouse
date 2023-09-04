WITH FINAL AS (
    SELECT
        D AS FULL_DATE,
        MD5(
            FORMAT_DATE(
                '%F',
                D
            )
        ) AS DATE_ID,
        EXTRACT(
            YEAR
            FROM
            D
        ) AS YEAR,
        EXTRACT(
            WEEK
            FROM
            D
        ) AS YEAR_WEEK,
        EXTRACT(
            DAYOFYEAR
            FROM
            D
        ) AS YEAR_DAY,
        EXTRACT(
            DAY
            FROM
            D
        ) AS MONTH_DAY,
        CASE
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    D
                ) > 7
                THEN (
                    EXTRACT(
                        YEAR
                        FROM
                        D
                    ) + 1
                )
            ELSE EXTRACT(
                YEAR
                FROM
                D
            )
        END AS FISCAL_YEAR,
        CASE
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    D
                ) > 7
                THEN EXTRACT(
                    MONTH
                    FROM
                    D
                ) - 7
            ELSE EXTRACT(
                MONTH
                FROM
                D
            ) + 5
        END AS FISCAL_PERIOD,
        CASE
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    D
                ) > 7
                AND EXTRACT(
                    MONTH
                    FROM
                    D
                ) < 11
                THEN (
                    1
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    D
                ) > 10
                AND EXTRACT(
                    MONTH
                    FROM
                    D
                ) < 13
                THEN (
                    2
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    D
                ) > 0
                AND EXTRACT(
                    MONTH
                    FROM
                    D
                ) < 2
                THEN (
                    2
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    D
                ) > 1
                AND EXTRACT(
                    MONTH
                    FROM
                    D
                ) < 5
                THEN (
                    3
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    D
                ) > 4
                AND EXTRACT(
                    MONTH
                    FROM
                    D
                ) < 8
                THEN (
                    4
                )
            ELSE 0
        END AS FISCAL_QTR,
        EXTRACT(
            MONTH
            FROM
            D
        ) AS MONTH,
        FORMAT_DATE(
            '%B',
            D
        ) AS MONTH_NAME,
        FORMAT_DATE(
            '%w',
            D
        ) AS WEEK_DAY,
        FORMAT_DATE(
            '%A',
            D
        ) AS DAY_NAME,
        (
            CASE
                WHEN FORMAT_DATE(
                    '%A',
                    D
                ) IN (
                    'Sunday',
                    'Saturday'
                ) THEN 0
                ELSE 1
            END
        ) AS DAY_IS_WEEKDAY
    FROM
        (
            SELECT *
            FROM
                UNNEST(
                    GENERATE_DATE_ARRAY(
                        '2008-01-01',
                        '2050-01-01',
                        INTERVAL 1 DAY
                    )
                ) AS D
        )
)

SELECT *
FROM
    FINAL
