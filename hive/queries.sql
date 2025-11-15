-- Analytical Queries for Twitter Climate Sentiment Pipeline

-- Query 1: Sentiment distribution
SELECT 
    sentiment_label,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_score,
    ROUND(AVG(sentiment_confidence), 3) as avg_confidence
FROM final_analytics
GROUP BY sentiment_label
ORDER BY tweet_count DESC;

-- Query 2: Negative sentiment by location (top 10)
SELECT 
    ROUND(geo_lat, 2) as lat,
    ROUND(geo_lon, 2) as lon,
    place_name,
    COUNT(*) as negative_tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment,
    ROUND(AVG(weather_temp_c), 1) as avg_temp_c,
    ROUND(AVG(weather_precip_mm), 1) as avg_precip_mm,
    ROUND(AVG(weather_wind_speed_ms), 1) as avg_wind_ms,
    MAX(weather_event_type) as primary_event
FROM final_analytics
WHERE sentiment_label = 'negative'
GROUP BY ROUND(geo_lat, 2), ROUND(geo_lon, 2), place_name
ORDER BY negative_tweet_count DESC
LIMIT 10;

-- Query 3: Average sentiment by weather event type
SELECT 
    weather_event_type,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment,
    ROUND(AVG(weather_temp_c), 1) as avg_temp_c,
    ROUND(AVG(weather_precip_mm), 1) as avg_precip_mm
FROM final_analytics
WHERE weather_event_type IS NOT NULL
GROUP BY weather_event_type
ORDER BY avg_sentiment ASC;

-- Query 4: Daily sentiment trend
SELECT 
    processing_date,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment,
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count
FROM final_analytics
GROUP BY processing_date
ORDER BY processing_date;

-- Query 5: Language distribution with sentiment
SELECT 
    detected_lang,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment,
    sentiment_method
FROM final_analytics
GROUP BY detected_lang, sentiment_method
ORDER BY tweet_count DESC
LIMIT 10;

-- Query 6: Geolocation quality analysis
SELECT 
    geo_confidence,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment,
    ROUND(AVG(weather_station_distance_km), 1) as avg_station_distance_km
FROM processed_tweets
GROUP BY geo_confidence
ORDER BY tweet_count DESC;

-- Query 7: NOAA match quality analysis
SELECT 
    noaa_match_quality,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment
FROM processed_tweets
WHERE noaa_match_quality IS NOT NULL
GROUP BY noaa_match_quality
ORDER BY tweet_count DESC;

-- Query 8: Emoji sentiment correlation
SELECT 
    CASE 
        WHEN emoji_count = 0 THEN 'no_emoji'
        WHEN emoji_count <= 2 THEN 'few_emoji'
        ELSE 'many_emoji'
    END as emoji_category,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment,
    ROUND(AVG(emoji_sentiment_score), 3) as avg_emoji_sentiment
FROM final_analytics
GROUP BY 
    CASE 
        WHEN emoji_count = 0 THEN 'no_emoji'
        WHEN emoji_count <= 2 THEN 'few_emoji'
        ELSE 'many_emoji'
    END
ORDER BY tweet_count DESC;

-- Query 9: Weather severity vs sentiment
SELECT 
    CASE 
        WHEN weather_temp_c > 35 THEN 'extreme_heat'
        WHEN weather_temp_c > 30 THEN 'hot'
        WHEN weather_temp_c < 0 THEN 'freezing'
        WHEN weather_temp_c < 10 THEN 'cold'
        ELSE 'moderate'
    END as temp_category,
    CASE 
        WHEN weather_precip_mm > 25 THEN 'heavy_rain'
        WHEN weather_precip_mm > 10 THEN 'rain'
        WHEN weather_precip_mm > 0 THEN 'light_rain'
        ELSE 'no_rain'
    END as precip_category,
    COUNT(*) as tweet_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment
FROM final_analytics
WHERE weather_temp_c IS NOT NULL
GROUP BY 
    CASE 
        WHEN weather_temp_c > 35 THEN 'extreme_heat'
        WHEN weather_temp_c > 30 THEN 'hot'
        WHEN weather_temp_c < 0 THEN 'freezing'
        WHEN weather_temp_c < 10 THEN 'cold'
        ELSE 'moderate'
    END,
    CASE 
        WHEN weather_precip_mm > 25 THEN 'heavy_rain'
        WHEN weather_precip_mm > 10 THEN 'rain'
        WHEN weather_precip_mm > 0 THEN 'light_rain'
        ELSE 'no_rain'
    END
ORDER BY tweet_count DESC;

-- Query 10: Export for visualization (geographic heatmap data)
SELECT 
    geo_lat,
    geo_lon,
    sentiment_score,
    sentiment_label,
    weather_event_type,
    text_clean,
    created_at
FROM final_analytics
WHERE 
    sentiment_label = 'negative'
    AND geo_lat IS NOT NULL
    AND geo_lon IS NOT NULL
ORDER BY sentiment_score ASC
LIMIT 1000;
