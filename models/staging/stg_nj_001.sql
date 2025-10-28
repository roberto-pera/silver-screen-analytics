with 

source as (

    select * from {{ source('raw', 'nj_001') }}

),

renamed as (

    select
        date_trunc('month', timestamp) as month,
        movie_id,
        'nj_001' as location_id,
        sum(ticket_amount) as total_tickets_sold,
        sum(transaction_total) as total_revenue
    from source
    group by 1, 2, 3

)

select * from renamed