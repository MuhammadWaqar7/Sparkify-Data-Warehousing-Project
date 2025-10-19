

CREATE OR REPLACE STAGE log_stage
  URL = 's3://sparkify-data-warehousing-project/input_data/log_data/'
  CREDENTIALS = (
    AWS_KEY_ID = ''
    AWS_SECRET_KEY = '')
  FILE_FORMAT = (TYPE = 'JSON');
LIST @log_stage;

-- Raw log data table
CREATE OR REPLACE TABLE raw_logs (
    json_data VARIANT,
    filename VARCHAR,
    load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO raw_logs (json_data, filename)
FROM (
    SELECT $1, METADATA$FILENAME
    FROM @log_stage
)
FILE_FORMAT = (TYPE = 'JSON');

-- Check log data count

SELECT COUNT(*) as log_files FROM raw_logs;
-- Check which tables have data
SELECT 'raw_songs' as table_name, COUNT(*) as record_count FROM raw_songs
UNION ALL
SELECT 'raw_logs', COUNT(*) FROM raw_logs;

-- Check song data structure (this should work)
SELECT 
    json_data:artist_id::STRING as artist_id,
    json_data:artist_name::STRING as artist_name,
    json_data:song_id::STRING as song_id,
    json_data:title::STRING as title,
    json_data:duration::FLOAT as duration
FROM raw_songs 
LIMIT 5;


-- Preview log data structure
SELECT 
    json_data:userId::INTEGER as user_id,
    json_data:firstName::STRING as first_name,
    json_data:song::STRING as song_name,
    json_data:artist::STRING as artist_name
FROM raw_logs 
LIMIT 5;
