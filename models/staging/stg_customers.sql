WITH countries AS (
    SELECT
        id AS source_country_id,
        name AS country_name,
        iso_code AS country_iso_code
    FROM
        {{ ref('countries') }}
),

customers AS (
    SELECT
        id AS source_customer_id,
        name AS customer_name,
        country_id AS source_customer_country_id,
        MD5(CAST(id AS STRING)) AS customer_id
    FROM
        {{ ref('customers') }}
),

final AS (
    SELECT
        customers.source_customer_id,
        customers.customer_name,
        countries.source_country_id AS source_customer_country_id,
        countries.country_name AS customer_country_name,
        countries.country_iso_code AS customer_country_iso_code,
        MD5(
            CONCAT(
                CAST(customers.source_customer_id AS STRING),
                CAST(
                    COALESCE(
                        CAST(countries.source_country_id AS STRING), ''
                    ) AS STRING
                )
            )
        ) AS customer_id
    FROM
        customers
    LEFT JOIN countries
        ON customers.source_customer_country_id = countries.source_country_id
)

SELECT *
FROM
    final
