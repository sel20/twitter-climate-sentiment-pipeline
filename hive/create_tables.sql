-- Hive DDL for Twitter Climate Sentiment Pipeline
-- Creates external tables over Parquet files

-- Drop existing tables if they exist
DROP TABLE IF EXISTS processed_tweets;
DROP TABLE IF EXISTS final_analytics;

-- Create external table for processed tweets
CREATE EXTERNAL TABLE IF NOT EXISTS processed_tweets (
    -- Identity fields
    tweet_id STRING,
    author_id STRING,
    created_at TIMESTAMP,
    created_at_str STRING,
    processing_ts TIMESTAMP,
    
    -- Text fields
    text_clean STRING,
    text_original STRING,
    detected_lang STRING,
    lang_confidence FLOAT,
    lang_method STRING,
    
    -- Text features
    hashtag_count INT,
    mention_count INT,
    url_count INT,
    emoji_count INT,
    emoji_sentiment_score FLOAT,
    text_length INT,
    word_count INT,
    has_media BOOLEAN,
    avg_word_length FLOAT,
    
    -- Geolocation fields
    geo_lat DOUBLE,
    geo_lon DOUBLE,
    geo_confidence STRING,
    geo_source STRING,
    place_name STRING,
    user_location_string STRING,
    
    -- Weather fields
    weather_station_id STRING,
    weather_station_name STRING,
    weather_station_distance_km FLOAT,
    weather_temp_c FLOAT,
    weather_temp_max_c FLOAT,
    weather_temp_min_c FLOAT,
    weather_precip_mm FLOAT,
    weather_wind_speed_ms FLOAT,
    weather_event_type STRING,
    weather_observation_time STRING,
    noaa_match_quality STRING,
    
    -- Sentiment fields
    sentiment_score FLOAT,
    sentiment_label STRING,
    sentiment_method STRING,
    sentiment_confidence FLOAT,
    
    -- Metadata
    raw_json_path STRING,
    source STRING,
    processing_stage STRING
)
PARTITIONED BY (processing_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/twitter_climate/processed_tweets/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Processed tweets with sentiment and weather enrichment'
);

-- Repair table to discover partitions
MSCK REPAIR TABLE processed_tweets;

-- Create final analytics table (filtered for high-quality records)
CREATE TABLE IF NOT EXISTS final_analytics
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    tweet_id,
    created_at,
    author_id,
    detected_lang,
    text_clean,
    
    -- Sentiment
    sentiment_score,
    sentiment_label,
    sentiment_method,
    sentiment_confidence,
    
    -- Geolocation
    geo_lat,
    geo_lon,
    geo_confidence,
    geo_source,
    place_name,
    
    -- Weather
    weather_station_id,
    weather_station_name,
    weather_station_distance_km,
    weather_temp_c,
    weather_precip_mm,
    weather_wind_speed_ms,
    weather_event_type,
    noaa_match_quality,
    
    -- Features
    hashtag_count,
    mention_count,
    emoji_count,
    emoji_sentiment_score,
    
    -- Metadata
    processing_date
FROM processed_tweets
WHERE 
    -- Filter for quality
    geo_confidence IN ('high', 'medium')
    AND weather_station_id IS NOT NULL
    AND sentiment_confidence > 0.5
    AND text_length > 10;

-- Show table info
DESCRIBE FORMATTED processed_tweets;
DESCRIBE FORMATTED final_analytics;

-- Show partition info
SHOW PARTITIONS processed_tweets;

-- Sample query
SELECT COUNT(*) as total_tweets FROM processed_tweets;
SELECT COUNT(*) as quality_tweets FROM final_analytics;
