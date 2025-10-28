SELECT
    cat.movie_id,
    cat.movie_title,
    cat.genre,
    inv.month,
    inv.location_id,
    inv.studio,
    inv.total_price
FROM {{ ref('stg_movie_catalogue') }} AS cat
RIGHT JOIN {{ ref('stg_invoices') }} AS inv
    ON cat.movie_id = inv.movie_id