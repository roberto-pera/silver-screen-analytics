with

    source as (select * from {{ source("raw", "movie_catalogue") }}),

    renamed as (

        select 
            movie_id, 
            upper(trim(movie_title)) as movie_title,
            coalesce(genre, 'Unknown') as genre, 
            upper(trim(studio)) as studio
        from source

    )

select *
from renamed
