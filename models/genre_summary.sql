select
    genre,
    count(*) as total_tracks,
    avg(popularity) as avg_popularity
from (
    select
        popularity,
        trim(genre) as genre
    from {{ ref('combine') }},
    unnest(split(artist_genres, ',')) as genre
)
group by genre
order by total_tracks desc
