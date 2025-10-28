-- tests/test_gross_margin_positive.sql
-- Purpose: Ensures that no movie-month-location combination has a negative gross margin

with invalid_rows as (

    select *
    from {{ ref('mart_movie_monthly_performance') }}
    where gross_margin < 0

)

select *
from invalid_rows