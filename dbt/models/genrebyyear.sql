select
    extract(year from release_date) as release_year,
    trim(genre) as genre,
    count(*) as total_tracks,
    avg(popularity) as avg_popularity
from (
    select
        popularity,
        release_date,
        trim(genre) as genre
    from {{ ref('combine') }},
    unnest(split(artist_genres, ',')) as genre
    where release_date is not null
)
group by release_year, genre
order by release_year, total_tracks desc
