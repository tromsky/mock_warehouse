with final as (

    select customer_id, source_customer_id, name
    from {{ ref('stg_customers') }}

)

select * from final