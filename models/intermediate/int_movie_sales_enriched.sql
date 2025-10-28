select
    cat.movie_id,
    cat.movie_title,
    cat.genre,
    inv.month,
    lower(inv.location_id) as location_id,
    inv.studio,
    inv.total_price
from {{ ref("stg_invoices") }} as inv
left join {{ ref("stg_movie_catalogue") }} as cat on cat.movie_id = inv.movie_id
