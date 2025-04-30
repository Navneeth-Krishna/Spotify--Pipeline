select
    extract(year from release_date) as release_year,
    count(*) as total_tracks,
    avg(popularity) as avg_popularity
from {{ ref('combine') }}
group by release_year
order by release_year
