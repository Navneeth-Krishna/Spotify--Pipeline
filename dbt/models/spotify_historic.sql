SELECT
  `track_name`,
  `track_uri`,
  popularity
FROM `spotify-458017.Top_songs.Historic_Table`
WHERE popularity > 50
