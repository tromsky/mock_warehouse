with final as (

    select 
       * 
    from {{ ref('stg_inventory_items') }} 

)

select * from final