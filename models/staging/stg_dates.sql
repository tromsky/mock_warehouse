WITH FINAL AS (
    SELECT
        MD5(
            format_date(
                '%F',
                d
            )
        ) AS date_id,
        d AS full_date,
        EXTRACT(
            YEAR
            FROM
                d
        ) AS YEAR,
        EXTRACT(
            week
            FROM
                d
        ) AS year_week,
        EXTRACT(
            dayofyear
            FROM
                d
        ) AS year_day,
        EXTRACT(
            DAY
            FROM
                d
        ) AS month_day,
        CASE
            WHEN EXTRACT(
                MONTH
                FROM
                    d
            ) > 7 THEN (
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
            WHEN EXTRACT(
                MONTH
                FROM
                    d
            ) > 7 THEN EXTRACT(
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
            WHEN EXTRACT(
                MONTH
                FROM
                    d
            ) > 7
            AND EXTRACT(
                MONTH
                FROM
                    d
            ) < 11 THEN (
                1
            )
            WHEN EXTRACT(
                MONTH
                FROM
                    d
            ) > 10
            AND EXTRACT(
                MONTH
                FROM
                    d
            ) < 13 THEN (
                2
            )
            WHEN EXTRACT(
                MONTH
                FROM
                    d
            ) > 0
            AND EXTRACT(
                MONTH
                FROM
                    d
            ) < 2 THEN (
                2
            )
            WHEN EXTRACT(
                MONTH
                FROM
                    d
            ) > 1
            AND EXTRACT(
                MONTH
                FROM
                    d
            ) < 5 THEN (
                3
            )
            WHEN EXTRACT(
                MONTH
                FROM
                    d
            ) > 4
            AND EXTRACT(
                MONTH
                FROM
                    d
            ) < 8 THEN (
                4
            )
            ELSE 0
        END AS fiscal_qtr,
        EXTRACT(
            MONTH
            FROM
                d
        ) AS MONTH,
        format_date(
            '%B',
            d
        ) AS month_name,
        format_date(
            '%w',
            d
        ) AS week_day,
        format_date(
            '%A',
            d
        ) AS day_name,
        (
            CASE
                WHEN format_date(
                    '%A',
                    d
                ) IN (
                    'Sunday',
                    'Saturday'
                ) THEN 0
                ELSE 1
            END
        ) AS day_is_weekday,
    FROM
        (
            SELECT
                *
            FROM
                unnest(
                    generate_date_array(
                        '2008-01-01',
                        '2050-01-01',
                        INTERVAL 1 DAY
                    )
                ) AS d
        )
)
SELECT
    *
FROM
    FINAL
