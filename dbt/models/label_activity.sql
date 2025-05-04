select
    label,
    count(*) as total_tracks,
    avg(popularity) as avg_popularity
from {{ ref('combine') }}
where label is not null
group by label
order by total_tracks desc
limit 20
