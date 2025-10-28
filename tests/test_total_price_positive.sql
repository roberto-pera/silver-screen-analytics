-- tests/test_total_price_positive.sql
-- Purpose: Ensures that total_price values are greater than zero and not null

with invalid_rows as (

    select *
    from {{ ref('mart_movie_monthly_performance') }}
    where total_price is null
       or total_price <= 0

)

select *
from invalid_rows
