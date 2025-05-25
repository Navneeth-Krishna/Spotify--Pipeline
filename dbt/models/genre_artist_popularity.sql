SELECT
  TRIM(genre) AS genre,
  TRIM(artist) AS artist_name,
  track_name,
  AVG(popularity) AS genre_popularity,
  MAX(popularity) AS track_popularity
FROM
  {{ ref('combine') }},
  UNNEST(SPLIT(artist_names, ',')) AS artist,
  UNNEST(SPLIT(artist_genres, ',')) AS genre
GROUP BY
  genre, artist_name, track_name
ORDER BY
  genre, genre_popularity DESC
