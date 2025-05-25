WITH base_data AS (
  SELECT
    track_uri,
    track_name,
    SPLIT(artist_names, ',') AS artist_list,
    label,
    CAST(DATE(release_date) AS DATE) AS release_date
  FROM {{ ref('combine') }}
),

exploded_artists AS (
  SELECT
    track_uri,
    track_name,
    TRIM(artist) AS artist_name,
    label,
    release_date,
    ARRAY_LENGTH(artist_list) - 1 AS collaboration_count  -- subtract 1 to exclude self
  FROM base_data,
  UNNEST(artist_list) AS artist
)

SELECT
  artist_name,
  label,
  track_name,
  track_uri,
  release_date,
  collaboration_count,
  COUNT(*) AS track_count
FROM exploded_artists
GROUP BY artist_name, label, track_name, track_uri, release_date, collaboration_count
ORDER BY artist_name, release_date DESC
