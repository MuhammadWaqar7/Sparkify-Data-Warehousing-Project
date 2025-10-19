-- Complete data quality summary
SELECT 
    'Raw Song Files' as metric, COUNT(*) as value FROM raw_songs
UNION ALL
SELECT 'Unique Artists', COUNT(*) FROM artists
UNION ALL
SELECT 'Unique Songs', COUNT(*) FROM songs
UNION ALL
SELECT 'Songs with Artist Match', COUNT(*) 
FROM songs s 
JOIN artists a ON s.artist_id = a.artist_id
UNION ALL
SELECT 'Songs without Artist Match', COUNT(*) 
FROM songs s 
LEFT JOIN artists a ON s.artist_id = a.artist_id 
WHERE a.artist_id IS NULL;

-- 1. Songs by year
SELECT 
    year,
    COUNT(*) as song_count
FROM songs
WHERE year > 0
GROUP BY year
ORDER BY year;

-- 2. Top artists by number of songs
SELECT 
    a.name as artist_name,
    a.location,
    COUNT(s.song_id) as total_songs
FROM artists a
JOIN songs s ON a.artist_id = s.artist_id
GROUP BY a.name, a.location
ORDER BY total_songs DESC
LIMIT 15;

-- 3. Song duration statistics
SELECT 
    MIN(duration) as min_duration,
    MAX(duration) as max_duration,
    AVG(duration) as avg_duration,
    COUNT(*) as total_songs
FROM songs
WHERE duration > 0;

-- 4. Artists with geographic data
SELECT 
    COUNT(*) as artists_with_location,
    COUNT(latitude) as artists_with_lat_long
FROM artists;


-- 5. Song duration distribution
SELECT 
    CASE 
        WHEN duration < 120 THEN 'Short (<2min)'
        WHEN duration BETWEEN 120 AND 300 THEN 'Medium (2-5min)'
        WHEN duration BETWEEN 300 AND 600 THEN 'Long (5-10min)'
        ELSE 'Very Long (>10min)'
    END as duration_category,
    COUNT(*) as song_count
FROM songs
WHERE duration > 0
GROUP BY duration_category
ORDER BY song_count DESC;

-- 6. Artists by region (extracted from location)
SELECT 
    CASE 
        WHEN location LIKE '%, US' OR location LIKE '%, USA' THEN 'United States'
        WHEN location LIKE '%, UK' THEN 'United Kingdom'
        WHEN location LIKE '%, CA' THEN 'Canada'
        WHEN location IS NOT NULL AND location != '' THEN 'Other Countries'
        ELSE 'Unknown'
    END as region,
    COUNT(*) as artist_count
FROM artists
GROUP BY region
ORDER BY artist_count DESC;

-- Comprehensive project summary
SELECT 
    (SELECT COUNT(*) FROM raw_songs) as raw_song_files,
    (SELECT COUNT(*) FROM artists) as artist_count,
    (SELECT COUNT(*) FROM songs) as song_count,
    (SELECT COUNT(*) FROM songs WHERE year > 0) as songs_with_valid_year,
    (SELECT COUNT(*) FROM artists WHERE location IS NOT NULL AND location != '') as artists_with_location,
    (SELECT ROUND(AVG(duration), 2) FROM songs WHERE duration > 0) as avg_song_duration_seconds;
