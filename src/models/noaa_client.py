"""
NOAA API Client - Interface for NOAA Climate Data Online (CDO) API.

This module provides functions to:
- Find nearest weather stations by coordinates
- Query weather observations for specific stations and time ranges
- Cache station metadata to reduce API calls
"""

import os
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import requests
import json

# Redis for caching
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("redis not available, NOAA caching disabled")

logger = logging.getLogger(__name__)


class NOAAClient:
    """
    Client for NOAA Climate Data Online (CDO) API v2.
    
    API Documentation: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
    """
    
    # NOAA API endpoints
    BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
    STATIONS_ENDPOINT = f"{BASE_URL}/stations"
    DATA_ENDPOINT = f"{BASE_URL}/data"
    
    # Rate limiting (1000 requests per day)
    MAX_REQUESTS_PER_DAY = 1000
    REQUEST_DELAY = 0.2  # 200ms between requests
    
    # Cache TTL
    STATION_CACHE_TTL = 24 * 60 * 60  # 24 hours
    
    # Dataset IDs
    DATASET_GHCND = "GHCND"  # Global Historical Climatology Network - Daily
    DATASET_PRECIP_HOURLY = "PRECIP_HLY"  # Precipitation Hourly
    
    def __init__(self, api_token: Optional[str] = None):
        """
        Initialize NOAA API client.
        
        Args:
            api_token: NOAA API token (or from NOAA_API_TOKEN env var)
        """
        self.api_token = api_token or os.getenv('NOAA_API_TOKEN')
        
        if not self.api_token:
            logger.warning("NOAA API token not configured")
        
        # Request tracking
        self.last_request_time = 0
        self.request_count = 0
        self.request_count_reset_time = time.time()
        
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
                self.cache.ping()
                logger.info(f"NOAA client: Redis cache connected")
            except Exception as e:
                logger.warning(f"NOAA client: Redis connection failed: {e}")
                self.cache = None
        
        logger.info("NOAA API client initialized")
    
    def find_nearest_station(
        self,
        lat: float,
        lon: float,
        max_distance_km: float = 50.0,
        dataset_id: str = DATASET_GHCND
    ) -> Optional[Dict[str, Any]]:
        """
        Find nearest weather station to given coordinates.
        
        Args:
            lat: Latitude
            lon: Longitude
            max_distance_km: Maximum search radius in kilometers
            dataset_id: NOAA dataset ID
            
        Returns:
            Station dict with id, name, distance, etc. or None
        """
        # Check cache first
        cache_key = f"noaa:station:{lat:.2f}:{lon:.2f}:{max_distance_km}"
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached:
                logger.debug(f"Station cache hit for ({lat:.2f}, {lon:.2f})")
                return json.loads(cached)
        
        try:
            # Convert km to degrees (approximate)
            # 1 degree latitude ≈ 111 km
            extent_degrees = max_distance_km / 111.0
            
            # Build request parameters
            params = {
                'datasetid': dataset_id,
                'extent': f"{lat - extent_degrees},{lon - extent_degrees},"
                         f"{lat + extent_degrees},{lon + extent_degrees}",
                'limit': 10,  # Get top 10 stations
                'sortfield': 'name'
            }
            
            # Make API request
            response = self._make_request(self.STATIONS_ENDPOINT, params)
            
            if not response or 'results' not in response:
                logger.warning(f"No stations found near ({lat:.2f}, {lon:.2f})")
                return None
            
            stations = response['results']
            
            if not stations:
                return None
            
            # Find closest station
            closest_station = None
            min_distance = float('inf')
            
            for station in stations:
                station_lat = station.get('latitude')
                station_lon = station.get('longitude')
                
                if station_lat is None or station_lon is None:
                    continue
                
                # Calculate distance (Haversine formula)
                distance = self._calculate_distance(lat, lon, station_lat, station_lon)
                
                if distance < min_distance and distance <= max_distance_km:
                    min_distance = distance
                    closest_station = {
                        'id': station['id'],
                        'name': station.get('name', 'Unknown'),
                        'latitude': station_lat,
                        'longitude': station_lon,
                        'elevation': station.get('elevation'),
                        'distance_km': round(distance, 2),
                        'dataset_id': dataset_id
                    }
            
            # Cache result
            if closest_station and self.cache:
                self.cache.setex(
                    cache_key,
                    self.STATION_CACHE_TTL,
                    json.dumps(closest_station)
                )
            
            return closest_station
        
        except Exception as e:
            logger.error(f"Error finding nearest station: {e}")
            return None
    
    def query_observations(
        self,
        station_id: str,
        start_time: datetime,
        end_time: datetime,
        dataset_id: str = DATASET_GHCND
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Query weather observations for a station and time range.
        
        Args:
            station_id: NOAA station ID
            start_time: Start datetime
            end_time: End datetime
            dataset_id: NOAA dataset ID
            
        Returns:
            List of observation dicts or None
        """
        try:
            # Format dates for API (YYYY-MM-DD)
            start_date = start_time.strftime('%Y-%m-%d')
            end_date = end_time.strftime('%Y-%m-%d')
            
            # Build request parameters
            params = {
                'datasetid': dataset_id,
                'stationid': station_id,
                'startdate': start_date,
                'enddate': end_date,
                'limit': 1000,  # Max results
                'units': 'metric'
            }
            
            # Make API request
            response = self._make_request(self.DATA_ENDPOINT, params)
            
            if not response or 'results' not in response:
                logger.debug(f"No observations found for station {station_id}")
                return None
            
            observations = response['results']
            
            # Parse observations
            parsed_observations = []
            for obs in observations:
                parsed = self._parse_observation(obs)
                if parsed:
                    parsed_observations.append(parsed)
            
            return parsed_observations if parsed_observations else None
        
        except Exception as e:
            logger.error(f"Error querying observations: {e}")
            return None
    
    def _parse_observation(self, obs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse raw NOAA observation into structured format.
        
        Args:
            obs: Raw observation dict from API
            
        Returns:
            Parsed observation dict or None
        """
        try:
            datatype = obs.get('datatype', '')
            value = obs.get('value')
            
            if value is None:
                return None
            
            # Map NOAA datatypes to our fields
            # Common datatypes: TMAX, TMIN, PRCP, SNOW, SNWD, AWND
            parsed = {
                'timestamp': obs.get('date'),
                'station_id': obs.get('station'),
                'datatype': datatype,
                'value': value,
                'attributes': obs.get('attributes', '')
            }
            
            # Convert to standard fields
            if datatype == 'TMAX':
                parsed['temperature_max_c'] = value / 10.0  # Tenths of degrees C
            elif datatype == 'TMIN':
                parsed['temperature_min_c'] = value / 10.0
            elif datatype == 'PRCP':
                parsed['precipitation_mm'] = value / 10.0  # Tenths of mm
            elif datatype == 'AWND':
                parsed['wind_speed_ms'] = value / 10.0  # Tenths of m/s
            elif datatype == 'SNOW':
                parsed['snowfall_mm'] = value
            
            return parsed
        
        except Exception as e:
            logger.warning(f"Error parsing observation: {e}")
            return None
    
    def _make_request(
        self,
        endpoint: str,
        params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request to NOAA API with rate limiting.
        
        Args:
            endpoint: API endpoint URL
            params: Query parameters
            
        Returns:
            Response JSON dict or None
        """
        if not self.api_token:
            logger.error("NOAA API token not configured")
            return None
        
        try:
            # Rate limiting
            self._apply_rate_limit()
            
            # Make request
            headers = {
                'token': self.api_token
            }
            
            response = requests.get(
                endpoint,
                headers=headers,
                params=params,
                timeout=30
            )
            
            # Track request
            self.request_count += 1
            
            # Handle rate limit
            if response.status_code == 429:
                logger.warning("NOAA API rate limit exceeded")
                return None
            
            # Handle errors
            if response.status_code != 200:
                logger.warning(f"NOAA API error: {response.status_code} - {response.text}")
                return None
            
            return response.json()
        
        except requests.exceptions.Timeout:
            logger.warning("NOAA API request timed out")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"NOAA API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in NOAA API request: {e}")
            return None
    
    def _apply_rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        
        # Reset daily counter if needed
        if current_time - self.request_count_reset_time > 86400:  # 24 hours
            self.request_count = 0
            self.request_count_reset_time = current_time
        
        # Check daily limit
        if self.request_count >= self.MAX_REQUESTS_PER_DAY:
            logger.warning("NOAA API daily request limit reached")
            time.sleep(60)  # Wait 1 minute
            return
        
        # Apply delay between requests
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - time_since_last)
        
        self.last_request_time = time.time()
    
    def _calculate_distance(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float
    ) -> float:
        """
        Calculate distance between two coordinates using Haversine formula.
        
        Args:
            lat1, lon1: First coordinate
            lat2, lon2: Second coordinate
            
        Returns:
            Distance in kilometers
        """
        from math import radians, sin, cos, sqrt, atan2
        
        # Earth radius in km
        R = 6371.0
        
        # Convert to radians
        lat1_rad = radians(lat1)
        lon1_rad = radians(lon1)
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)
        
        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        distance = R * c
        return distance


if __name__ == "__main__":
    # For local testing
    
    # Test with Houston coordinates
    client = NOAAClient()
    
    print("Testing NOAA API Client\n")
    
    # Test 1: Find nearest station
    print("Test 1: Finding nearest station to Houston, TX")
    lat, lon = 29.7604, -95.3698
    station = client.find_nearest_station(lat, lon, max_distance_km=50)
    
    if station:
        print(f"  ✓ Found station: {station['name']}")
        print(f"    ID: {station['id']}")
        print(f"    Distance: {station['distance_km']} km")
        print(f"    Coordinates: ({station['latitude']}, {station['longitude']})")
        
        # Test 2: Query observations
        print("\nTest 2: Querying recent observations")
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=7)
        
        observations = client.query_observations(
            station['id'],
            start_time,
            end_time
        )
        
        if observations:
            print(f"  ✓ Found {len(observations)} observations")
            print(f"    Sample: {observations[0]}")
        else:
            print("  ✗ No observations found")
    else:
        print("  ✗ No station found")
