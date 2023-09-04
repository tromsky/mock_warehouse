WITH final AS (
    SELECT
        d AS full_date,
        MD5(
            FORMAT_DATE(
                '%F',
                d
            )
        ) AS date_id,
        EXTRACT(
            YEAR
            FROM
            d
        ) AS year,
        EXTRACT(
            WEEK
            FROM
            d
        ) AS year_week,
        EXTRACT(
            DAYOFYEAR
            FROM
            d
        ) AS year_day,
        EXTRACT(
            DAY
            FROM
            d
        ) AS month_day,
        CASE
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    d
                ) > 7
                THEN (
                    EXTRACT(
                        YEAR
                        FROM
                        d
                    ) + 1
                )
            ELSE EXTRACT(
                YEAR
                FROM
                d
            )
        END AS fiscal_year,
        CASE
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    d
                ) > 7
                THEN EXTRACT(
                    MONTH
                    FROM
                    d
                ) - 7
            ELSE EXTRACT(
                MONTH
                FROM
                d
            ) + 5
        END AS fiscal_period,
        CASE
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    d
                ) > 7
                AND EXTRACT(
                    MONTH
                    FROM
                    d
                ) < 11
                THEN (
                    1
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    d
                ) > 10
                AND EXTRACT(
                    MONTH
                    FROM
                    d
                ) < 13
                THEN (
                    2
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    d
                ) > 0
                AND EXTRACT(
                    MONTH
                    FROM
                    d
                ) < 2
                THEN (
                    2
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    d
                ) > 1
                AND EXTRACT(
                    MONTH
                    FROM
                    d
                ) < 5
                THEN (
                    3
                )
            WHEN
                EXTRACT(
                    MONTH
                    FROM
                    d
                ) > 4
                AND EXTRACT(
                    MONTH
                    FROM
                    d
                ) < 8
                THEN (
                    4
                )
            ELSE 0
        END AS fiscal_qtr,
        EXTRACT(
            MONTH
            FROM
            d
        ) AS month,
        FORMAT_DATE(
            '%B',
            d
        ) AS month_name,
        FORMAT_DATE(
            '%w',
            d
        ) AS week_day,
        FORMAT_DATE(
            '%A',
            d
        ) AS day_name,
        (
            CASE
                WHEN FORMAT_DATE(
                    '%A',
                    d
                ) IN (
                    'Sunday',
                    'Saturday'
                ) THEN 0
                ELSE 1
            END
        ) AS day_is_weekday
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
                ) AS d
        )
)

SELECT *
FROM
    final
