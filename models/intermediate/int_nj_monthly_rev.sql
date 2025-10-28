WITH combined AS (

    SELECT 
        month,
        movie_id,
        location_id,
        total_tickets_sold,
        total_revenue
    FROM {{ ref('stg_nj_001') }}

    UNION ALL

    SELECT 
        month,
        movie_id,
        location_id,
        total_tickets_sold,
        total_revenue
    FROM {{ ref('stg_nj_002') }}

    UNION ALL

    SELECT 
        month,
        movie_id,
        location_id,
        total_tickets_sold,
        total_revenue
    FROM {{ ref('stg_nj_003') }}
)

SELECT * 
FROM combined
