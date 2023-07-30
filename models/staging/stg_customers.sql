WITH countries AS (
    SELECT
        id AS source_country_id,
        NAME AS country_name,
        iso_code AS country_iso_code
    FROM
        {{ ref('countries') }}
),
customers AS (
    SELECT
        MD5(CAST(id AS STRING)) AS customer_id,
        id AS source_customer_id,
        NAME AS customer_name,
        country_id AS source_customer_country_id
    FROM
        {{ ref('customers') }}
),
FINAL AS (
    SELECT
        MD5(
            CONCAT(CAST(customers.source_customer_id AS STRING), CAST(COALESCE(CAST(countries.source_country_id AS STRING), '') AS STRING))
        ) AS customer_id,
        customers.source_customer_id,
        customers.customer_name,
        countries.source_country_id AS source_customer_country_id,
        countries.country_name AS customer_country_name,
        countries.country_iso_code AS customer_country_iso_code
    FROM
        customers
        LEFT JOIN countries
        ON customers.source_customer_country_id = countries.source_country_id
)
SELECT
    *
FROM
    FINAL
