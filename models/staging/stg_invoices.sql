with 

source as (

    select * from {{ source('raw', 'invoices') }}

),

renamed as (

    select
        movie_id,
        month,
        location_id,
        studio,
        min(total_invoice_sum) as total_price
    from source
    group by 1, 2, 3, 4
)

select * from renamed