with 

source as (

    select * from {{ source('raw', 'nj_002') }}

),

renamed as (

    select
        date_trunc('month', date) as month,
        movie_id,
        'nj_002' as location_id,
        sum(ticket_amount) as total_tickets_sold,
        sum(total_earned) as total_revenue
    from source
    group by 1, 2, 3

)

select * from renamed