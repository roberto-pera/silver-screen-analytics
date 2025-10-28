select 
    rev.movie_id,
    s.movie_title,
    s.genre,
    s.studio,
    rev.month,
    rev.location_id,
    rev.total_tickets_sold,
    rev.total_revenue,
    s.total_price,
    round(rev.total_revenue/s.total_price, 2) as gross_margin

from {{ ref('int_nj_monthly_rev') }} rev
join {{ ref('int_movie_sales_enriched') }} s
on rev.movie_id = s.movie_id and rev.location_id = s.location_id and rev.month = s.month