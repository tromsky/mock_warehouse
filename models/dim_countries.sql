with final as (

    select country_id, source_country_id, name, iso_code
    from {{ ref('stg_countries') }}

)

select * from final