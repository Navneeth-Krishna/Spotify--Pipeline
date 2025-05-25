SELECT
  TRIM(genre) AS genre,
  TRIM(artist) AS artist_name,
  track_name,
  popularity
FROM
  {{ ref('combine') }},
  UNNEST(SPLIT(artist_names, ',')) AS artist,
  UNNEST(SPLIT(artist_genres, ',')) AS genre
