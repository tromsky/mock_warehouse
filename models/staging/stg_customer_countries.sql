with customers as (

    select * from {{ ref('stg_customers') }} 

),
countries as (

    select * from {{ ref('stg_countries') }}

),
final as (

    select

        md5(concat(customers.customer_id, countries.country_id)) as customer_country_id,
        customers.source_customer_id, 
        customers.name as customer_name,
        countries.source_country_id,
        countries.name as country_name,
        countries.iso_code
    
    from customers
    join countries
        on customers.source_country_id = countries.source_country_id

)

select * from final