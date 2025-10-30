with

    source as (select * from {{ source("raw", "movie_catalogue") }}),

    renamed as (

        select 
            movie_id, 
            movie_title,
            coalesce(genre, 'Unknown') as genre, 
            studio
        from source

    )

select *
from renamed
