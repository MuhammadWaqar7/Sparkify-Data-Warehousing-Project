create DATABASE data_Sparkify;
create schema song;

-- Create a medium warehouse 
CREATE OR REPLACE WAREHOUSE my_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60       -- suspend after 60 seconds of inactivity
  AUTO_RESUME = TRUE      -- resume automatically when a query runs
  MIN_CLUSTER_COUNT = 1   -- autoscaling min
  MAX_CLUSTER_COUNT = 3   -- autoscaling max
  SCALING_POLICY = 'STANDARD';

-- Use the warehouse
USE WAREHOUSE my_wh;


-- song table TABLE
drop table if exists song_data;
-- Drop the old stage if it exists
DROP STAGE IF EXISTS sparkify_log_stage;

-- Create JSON file format for your song data

CREATE OR REPLACE FILE FORMAT json_format 
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;,
---create stage
CREATE OR REPLACE STAGE sparkify_log_stage
  URL = 's3://sparkify-data-warehousing-project/input_data/log_data/'
  CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
  REGION = 'us-west-2'  -- Add your bucket's region
  FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

-- List files to verify the stage works
LIST @sparkify_log_stage;
CREATE OR REPLACE TABLE songplays (
    songplay_id INTEGER AUTOINCREMENT PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);

COPY INTO songplays_log_data (json_data, filename)
FROM (
    SELECT 
        $1 AS json_data,
        METADATA$FILENAME AS filename
    FROM @sparkify_log_stage
)
FILE_FORMAT = (FORMAT_NAME = 'json_format');




CREATE OR REPLACE STORAGE INTEGRATION sparkify_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/your-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sparkify-data-warehousing-project/input_data/');

-- Create the stage (this looks correct)

CREATE OR REPLACE TABLE songplays_log_data (
    json_data VARIANT,
    filename VARCHAR,
    load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
COPY INTO songplays_log_data (json_data, filename)
FROM (
    SELECT 
        $1 AS json_data,
        METADATA$FILENAME AS filename
    FROM @sparkify_log_stage
)
FILE_FORMAT = (FORMAT_NAME = 'json_format');
LIST @sparkify_log_stage;
SELECT COUNT(*) as total_records FROM songplays_log_data;

-- See the structure of your JSON data
-- See what your JSON data actually looks like
SELECT 
    json_data,
    filename
FROM songplays_log_data 
LIMIT 3;

-- Check specific fields
SELECT 
    json_data:artist_id::STRING as artist_id,
    json_data:artist_name::STRING as artist_name,
    json_data:song_id::STRING as song_id,
    json_data:title::STRING as title
FROM songplays_log_data 
LIMIT 5;
-- artists TABLE

CREATE OR REPLACE TABLE artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);

INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    json_data:artist_id::VARCHAR as artist_id,
    json_data:artist_name::VARCHAR as name,
    json_data:artist_location::VARCHAR as location,
    json_data:artist_latitude::FLOAT as latitude,
    json_data:artist_longitude::FLOAT as longitude
FROM songplays_log_data
WHERE json_data:artist_id IS NOT NULL
  AND json_data:artist_id != ''
  AND artist_id NOT IN (SELECT artist_id FROM artists);
  -- Avoid duplicates

---CREATE OR REPLACE TABLE songs  
CREATE OR REPLACE TABLE songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration FLOAT,
    num_songs INTEGER,
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);

--- insert data into song
INSERT INTO songs (song_id, title, artist_id, year, duration, num_songs)
SELECT DISTINCT
    json_data:song_id::VARCHAR as song_id,
    json_data:title::VARCHAR as title,
    json_data:artist_id::VARCHAR as artist_id,
    json_data:year::INTEGER as year,
    json_data:duration::FLOAT as duration,
    json_data:num_songs::INTEGER as num_songs
FROM songplays_log_data
WHERE json_data:song_id IS NOT NULL
  AND json_data:song_id != ''
  AND json_data:title IS NOT NULL
  AND song_id NOT IN (SELECT song_id FROM songs); -- Avoid duplicates

  SELECT 
    (SELECT COUNT(*) FROM artists) as artist_count,
    (SELECT COUNT(*) FROM songs) as song_count;
-- See sample data from artists table
SELECT * FROM artists LIMIT 5;

-- See songs with artist information

SELECT 
    s.song_id,
    s.title as song_title,
    s.duration,
    s.year,
    a.name as artist_name,
    a.location as artist_location
FROM songs s
JOIN artists a ON s.artist_id = a.artist_id
LIMIT 10;
  
  
 -- ========
  -- CUSTOMER TABLE

create or replace table "CUSTOMER"(
    "customer_id" string primary key,
    "average_stars" double,
    "fans" number,
    "review_count" number,
    "name" string
);

insert into "CUSTOMER"
select 
    RECORDJSON:user_id,
    RECORDJSON:average_stars,
    RECORDJSON:fans,
    RECORDJSON:review_count,
    RECORDJSON:name
from 
    data_design.staging.YELP_ACADEMIC_DATASET_USER;

-- COVID TABLE

create or replace table "COVID"(
  "business_id" string primary key references data_design.ODS."BUSINESS"("business_id"),
  "call_action" string,
  "covid_banner" string,
  "grubhub" string,
  "request_a_quote" string,
  "temporary_closed" string,
  "virtual_services" string,
  "delivery_or_takeout" string,
  "highlights" string
);

insert into "COVID"
select 
    RECORDJSON:"Call To Action enabled",
    RECORDJSON:"Covid Banner",
    RECORDJSON:"Grubhub enabled",
    RECORDJSON:"Request a Quote Enabled",
    RECORDJSON:"Temporary Closed Until",
    RECORDJSON:"Virtual Services Offered",
    RECORDJSON:business_id,
    RECORDJSON:"delivery or takeout",
    RECORDJSON:highlights
from 
    data_design.staging.YELP_ACADEMIC_DATASET_COVID_FEATURES;

create or replace table "REVIEW"(
    "review_id" string primary key,
    "business_id" string references data_design.ODS."BUSINESS"("business_id"),
    "date" date,
    "cool" number,
    "funny" number,
    "stars" double,
    "useful" double,
    "user_id" string references data_design.ODS."CUSTOMER"("customer_id")
);

-- REVIEW TABLE
insert into "REVIEW"
select
    RECORDJSON:review_id,
    RECORDJSON:business_id,
    RECORDJSON:date::date,
    RECORDJSON:cool,
    RECORDJSON:funny,
    RECORDJSON:stars,
    RECORDJSON:useful,
    RECORDJSON:user_id
from 
    data_design.staging.YELP_ACADEMIC_DATASET_REVIEW;


-- TIP TABLE
create or replace table "TIP"(
  "business_id" string primary key references data_design.ODS."BUSINESS"("business_id"),
  "compliment_count" number,
  "date" date,
  "user_id" string references data_design.ODS."CUSTOMER"("customer_id")
);

insert into "TIP"
select 
    RECORDJSON:business_id,
    RECORDJSON:compliment_count,
    RECORDJSON:date,
    RECORDJSON:user_id
from 
    data_design.staging.YELP_ACADEMIC_DATASET_TIP;

-- TEMPERATURE TABLE

create or replace table "TEMPERATURE"(
    "date" date primary key,
    "min_temp" double,
    "max_temp" double,
    "normal_min" double,
    "normal_max" double
);

insert into "TEMPERATURE"(
    "date", "min_temp", "max_temp", "normal_min", "normal_max"
)
select 
    date Date,
    min double,
    max double,
    normal_min double,
    normal_max double
from 
    data_design.staging.lv_temperature;


-- PRECIPITATION TABLE

create or replace table "PRECIPITATION"(
    "date" date primary key,
    "precipitation" string,
    "precipitation_normal" double
);

insert into "PRECIPITATION"(
    "date", "precipitation", "precipitation_normal"
)
select 
    date,
    precipitation,
    precipitation_normal
from 
    data_design.staging.lv_precipitation;
