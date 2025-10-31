select
    rev.movie_id,
    inv.movie_title,
    inv.genre,
    inv.studio,
    rev.month,
    rev.location_id as location,
    rev.total_tickets_sold as tickets_sold,
    rev.total_revenue as revenue,
    inv.total_price as rental_cost,
    round((rev.total_revenue - inv.total_price) / nullif(rev.total_revenue, 0), 2) as gross_margin

from {{ ref("int_nj_monthly_rev") }} rev
join
    {{ ref("int_invoices_enriched") }} inv
    on rev.movie_id = inv.movie_id
    and rev.location_id = inv.location_id
    and rev.month = inv.month
