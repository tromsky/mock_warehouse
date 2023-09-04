WITH final AS (
    SELECT
        customer_id,
        source_customer_id,
        customer_name,
        source_customer_country_id,
        customer_country_name,
        customer_country_iso_code
    FROM

        {{ ref('stg_customers') }}
)

SELECT *
FROM
    final
