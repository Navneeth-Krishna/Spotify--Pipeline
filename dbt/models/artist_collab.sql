-- models/artist_collab.sql

with exploded as (
  select
    track_uri,
    track_name,
    trim(artist) as artist_name
  from {{ ref('combine') }},
  unnest(split(artist_names, ',')) as artist
),

paired as (
  select
    a.track_uri,
    a.track_name,
    a.artist_name as artist_a,
    b.artist_name as artist_b
  from exploded a
  join exploded b
    on a.track_uri = b.track_uri
    and a.artist_name < b.artist_name
)

select
  artist_a,
  artist_b,
  count(distinct track_uri) as collaboration_count,
  string_agg(distinct track_name, ', ') as track_names
from paired
group by artist_a, artist_b
