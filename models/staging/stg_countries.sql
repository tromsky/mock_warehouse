with final as (

    select 
        md5(cast(id as string)) as country_id,
        id as source_country_id,
        name,
        iso_code
    from {{ ref('countries') }} 

)

select * from final