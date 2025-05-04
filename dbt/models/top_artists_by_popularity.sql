-- select
--     artist_names,
--     avg(popularity) as avg_popularity
-- from {{ ref('combine') }}
-- group by artist_names


select
  trim(artist) as artist_name,
  avg(popularity) as avg_popularity,
  count(distinct track_uri) as track_count
from
  `spotify-458017`.`spotify_dbt`.`combine`,
  unnest(split(artist_names, ',')) as artist
group by artist_name
order by avg_popularity desc
