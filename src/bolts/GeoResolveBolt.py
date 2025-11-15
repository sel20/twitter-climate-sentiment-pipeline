"""
GeoResolveBolt - Extracts or infers geolocation with confidence scoring.

This bolt implements a three-tier geolocation strategy:
1. Direct coordinates (high confidence)
2. Place bounding box centroid (medium confidence)
3. Geocode user location string (low confidence)
"""

import os
import logging
import time
from typing import Dict, Any, Optional, Tuple
import json

# Geocoding library
try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False
    logging.warning("geopy not available, geocoding disabled")

# Redis for caching
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("redis not available, geocoding cache disabled")

# Storm imports
try:
    from streamparse.bolt import Bolt
except ImportError:
    class Bolt:
        def initialize(self, conf, ctx):
            pass
        def process(self, tup):
            pass

logger = logging.getLogger(__name__)


class GeoResolveBolt(Bolt):
    """
    Storm bolt that resolves tweet geolocation with confidence levels.
    
    Features:
    - Three-tier geolocation strategy
    - Confidence scoring (high/medium/low/unavailable)
    - Geocoding with Nominatim (OpenStreetMap)
    - Redis caching for geocoded locations (7-day TTL)
    - Rate limiting for geocoding API
    """
    
    # Geocoding rate limit (1 request per second for Nominatim)
    GEOCODING_DELAY = 1.0
    
    # Cache TTL (7 days)
    CACHE_TTL = 7 * 24 * 60 * 60
    
    def initialize(self, conf, ctx):
        """Initialize the bolt."""
        self.conf = conf
        self.ctx = ctx
        
        # Initialize geocoder
        self.geocoder = None
        if GEOPY_AVAILABLE:
            self.geocoder = Nominatim(
                user_agent="twitter_climate_sentiment_pipeline",
                timeout=10
            )
            logger.info("Nominatim geocoder initialized")
        
        # Initialize Redis cache
        self.cache = None
        if REDIS_AVAILABLE:
            try:
                redis_host = os.getenv('REDIS_HOST', 'localhost')
                redis_port = int(os.getenv('REDIS_PORT', '6379'))
                redis_db = int(os.getenv('REDIS_DB', '0'))
                
                self.cache = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    db=redis_db,
                    decode_responses=True
                )
                # Test connection
                self.cache.ping()
                logger.info(f"Redis cache connected: {redis_host}:{redis_port}")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}")
                self.cache = None
        
        # Rate limiting
        self.last_geocode_time = 0
        
        # Statistics
        self.processed = 0
        self.failed = 0
        self.high_confidence = 0
        self.medium_confidence = 0
        self.low_confidence = 0
        self.unavailable = 0
        self.geocode_cache_hits = 0
        self.geocode_api_calls = 0
        
        logger.info("GeoResolveBolt initialized")
        logger.info(f"  Geocoder available: {GEOPY_AVAILABLE}")
        logger.info(f"  Cache available: {self.cache is not None}")
    
    def process(self, tup):
        """
        Process incoming tuple from LangDetectBolt.
        
        Args:
            tup: Tuple containing language-detected tweet data
        """
        try:
            # Extract tweet data
            tweet_data = tup.values[0]
            
            # Resolve geolocation
            geo_data = self._resolve_geolocation(tweet_data)
            
            if not geo_data:
                logger.error(f"Failed to resolve geolocation for tweet {tweet_data.get('tweet_id')}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Add geo data to tweet
            enriched_data = tweet_data.copy()
            enriched_data.update(geo_data)
            enriched_data['processing_stage'] = 'geo_resolved'
            
            # Emit enriched tuple
            self.emit([enriched_data], anchors=[tup])
            self.ack(tup)
            
            self.processed += 1
            if self.processed % 1000 == 0:
                self._log_statistics()
        
        except Exception as e:
            logger.error(f"Error processing tuple: {e}", exc_info=True)
            self.fail(tup)
            self.failed += 1
    
    def _resolve_geolocation(self, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Resolve geolocation using three-tier strategy.
        
        Args:
            tweet_data: Tweet data
            
        Returns:
            Dict with geo fields or None
        """
        try:
            # Priority 1: Direct coordinates (highest confidence)
            coordinates = tweet_data.get('coordinates')
            if coordinates and isinstance(coordinates, dict):
                lat = coordinates.get('lat')
                lon = coordinates.get('lon')
                if lat is not None and lon is not None:
                    self.high_confidence += 1
                    return {
                        'geo_lat': float(lat),
                        'geo_lon': float(lon),
                        'geo_confidence': 'high',
                        'geo_source': 'coordinates'
                    }
            
            # Priority 2: Place bounding box centroid (medium confidence)
            place = tweet_data.get('place')
            if place and isinstance(place, dict):
                centroid = self._compute_place_centroid(place)
                if centroid:
                    self.medium_confidence += 1
                    return {
                        'geo_lat': centroid[0],
                        'geo_lon': centroid[1],
                        'geo_confidence': 'medium',
                        'geo_source': 'place_centroid',
                        'place_name': place.get('full_name', 'unknown')
                    }
            
            # Priority 3: Geocode user location string (low confidence)
            user_location = tweet_data.get('user_location')
            if user_location and isinstance(user_location, str) and user_location.strip():
                geocoded = self._geocode_location(user_location.strip())
                if geocoded:
                    self.low_confidence += 1
                    return {
                        'geo_lat': geocoded[0],
                        'geo_lon': geocoded[1],
                        'geo_confidence': 'low',
                        'geo_source': 'user_location_geocoded',
                        'user_location_string': user_location
                    }
            
            # No geolocation available
            self.unavailable += 1
            return {
                'geo_lat': None,
                'geo_lon': None,
                'geo_confidence': 'unavailable',
                'geo_source': 'unavailable'
            }
        
        except Exception as e:
            logger.error(f"Error resolving geolocation: {e}")
            return None
    
    def _compute_place_centroid(self, place: Dict[str, Any]) -> Optional[Tuple[float, float]]:
        """
        Compute centroid of place bounding box.
        
        Args:
            place: Place dict with geo.bbox field
            
        Returns:
            (lat, lon) tuple or None
        """
        try:
            geo = place.get('geo', {})
            
            # Twitter API v2 format: bbox array [lon_min, lat_min, lon_max, lat_max]
            bbox = geo.get('bbox')
            if bbox and len(bbox) == 4:
                lon_min, lat_min, lon_max, lat_max = bbox
                centroid_lat = (lat_min + lat_max) / 2
                centroid_lon = (lon_min + lon_max) / 2
                return (centroid_lat, centroid_lon)
            
            # Alternative format: coordinates array of [lon, lat] pairs
            coordinates = geo.get('coordinates')
            if coordinates and isinstance(coordinates, list):
                # Handle nested arrays
                if isinstance(coordinates[0], list):
                    coordinates = coordinates[0]
                
                # Compute average of all points
                if len(coordinates) > 0:
                    lats = [coord[1] for coord in coordinates if len(coord) >= 2]
                    lons = [coord[0] for coord in coordinates if len(coord) >= 2]
                    
                    if lats and lons:
                        centroid_lat = sum(lats) / len(lats)
                        centroid_lon = sum(lons) / len(lons)
                        return (centroid_lat, centroid_lon)
            
            return None
        
        except Exception as e:
            logger.warning(f"Error computing place centroid: {e}")
            return None
    
    def _geocode_location(self, location_string: str) -> Optional[Tuple[float, float]]:
        """
        Geocode location string using Nominatim with caching.
        
        Args:
            location_string: User location string
            
        Returns:
            (lat, lon) tuple or None
        """
        if not self.geocoder:
            return None
        
        try:
            # Check cache first
            if self.cache:
                cache_key = f"geocode:{location_string.lower()}"
                cached = self.cache.get(cache_key)
                if cached:
                    self.geocode_cache_hits += 1
                    coords = json.loads(cached)
                    return (coords['lat'], coords['lon'])
            
            # Rate limiting
            current_time = time.time()
            time_since_last = current_time - self.last_geocode_time
            if time_since_last < self.GEOCODING_DELAY:
                time.sleep(self.GEOCODING_DELAY - time_since_last)
            
            # Geocode using Nominatim
            location = self.geocoder.geocode(location_string)
            self.last_geocode_time = time.time()
            self.geocode_api_calls += 1
            
            if location:
                coords = (location.latitude, location.longitude)
                
                # Cache result
                if self.cache:
                    cache_key = f"geocode:{location_string.lower()}"
                    cache_value = json.dumps({'lat': coords[0], 'lon': coords[1]})
                    self.cache.setex(cache_key, self.CACHE_TTL, cache_value)
                
                return coords
            
            return None
        
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"Geocoding service error for '{location_string}': {e}")
            return None
        except Exception as e:
            logger.error(f"Error geocoding location '{location_string}': {e}")
            return None
    
    def _log_statistics(self):
        """Log geolocation resolution statistics."""
        total = self.processed
        if total == 0:
            return
        
        high_pct = (self.high_confidence / total * 100) if total > 0 else 0
        medium_pct = (self.medium_confidence / total * 100) if total > 0 else 0
        low_pct = (self.low_confidence / total * 100) if total > 0 else 0
        unavail_pct = (self.unavailable / total * 100) if total > 0 else 0
        
        cache_hit_rate = 0
        if self.geocode_api_calls + self.geocode_cache_hits > 0:
            cache_hit_rate = (self.geocode_cache_hits / 
                            (self.geocode_api_calls + self.geocode_cache_hits) * 100)
        
        logger.info(
            f"GeoResolveBolt stats: "
            f"processed={self.processed}, "
            f"failed={self.failed}, "
            f"high={self.high_confidence} ({high_pct:.1f}%), "
            f"medium={self.medium_confidence} ({medium_pct:.1f}%), "
            f"low={self.low_confidence} ({low_pct:.1f}%), "
            f"unavailable={self.unavailable} ({unavail_pct:.1f}%), "
            f"geocode_calls={self.geocode_api_calls}, "
            f"cache_hits={self.geocode_cache_hits} ({cache_hit_rate:.1f}%)"
        )


if __name__ == "__main__":
    # For local testing
    
    bolt = GeoResolveBolt()
    bolt.initialize({}, {})
    
    # Test cases
    test_tweets = [
        {
            'tweet_id': '1',
            'coordinates': {'lat': 29.7604, 'lon': -95.3698},
            'place': None,
            'user_location': None
        },
        {
            'tweet_id': '2',
            'coordinates': None,
            'place': {
                'full_name': 'Miami, FL',
                'geo': {
                    'type': 'Feature',
                    'bbox': [-80.3182, 25.7090, -80.1918, 25.8557]
                }
            },
            'user_location': None
        },
        {
            'tweet_id': '3',
            'coordinates': None,
            'place': None,
            'user_location': 'New York, NY'
        },
        {
            'tweet_id': '4',
            'coordinates': None,
            'place': None,
            'user_location': None
        }
    ]
    
    print("Testing GeoResolveBolt:\n")
    for tweet in test_tweets:
        print(f"Tweet {tweet['tweet_id']}:")
        result = bolt._resolve_geolocation(tweet)
        if result:
            print(f"  Lat/Lon: {result.get('geo_lat')}, {result.get('geo_lon')}")
            print(f"  Confidence: {result['geo_confidence']}")
            print(f"  Source: {result['geo_source']}")
        print()
    
    bolt._log_statistics()
