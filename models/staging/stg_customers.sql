with final as (

    select 
        md5(cast(id as string)) as customer_id,
        id as source_customer_id,
        name,
        country_id as source_country_id
    from {{ ref('customers') }} 

)

select * from final