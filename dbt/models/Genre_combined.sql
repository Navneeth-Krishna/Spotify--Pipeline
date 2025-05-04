-- Replace `your_dataset.your_table` with your actual table
WITH exploded_genres AS (
  SELECT
    track_name,
    TRIM(genre) AS final_genre
  FROM{{ ref('combine') }}
    ,
    UNNEST(SPLIT(artist_genres, ',')) AS genre
)

-- Remove duplicates
SELECT DISTINCT
  track_name,
  final_genre
FROM
  exploded_genres
WHERE
  final_genre IS NOT NULL AND final_genre != ''
