-- select
--     artist_names,
--     count(distinct track_uri) as track_count
-- from {{ ref('combine') }}
-- group by artist_names
-- order by track_count desc
-- limit 20

select
    trim(artist) as artist_name,
    count(distinct track_uri) as track_count
from
    {{ ref('combine') }},
    unnest(split(artist_names, ',')) as artist
group by artist_name
order by track_count desc
limit 20
