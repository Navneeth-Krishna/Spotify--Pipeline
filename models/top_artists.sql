select
    artist_names,
    count(distinct track_uri) as track_count
from {{ ref('combine') }}
group by artist_names
order by track_count desc
limit 20