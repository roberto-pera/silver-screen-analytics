with 

source as (

    select * from {{ source('raw', 'nj_003') }}

),

renamed as (

    select
        date_trunc('month', timestamp) as month,
        details as movie_id,
        'nj_003' as location_id,
        sum(amount) as total_tickets_sold,
        sum(total_value) as total_revenue
    from source
    where product_type = 'ticket'
    group by 1, 2, 3

)

select * from renamed