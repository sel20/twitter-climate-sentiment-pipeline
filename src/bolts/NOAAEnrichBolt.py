"""
NOAAEnrichBolt - Enriches tweets with NOAA weather data.

This bolt queries NOAA API for weather observations matching the tweet's
location and timestamp, adding weather metrics to the tweet data.
"""

import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import sys

# Add models to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from models.noaa_client import NOAAClient

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


class NOAAEnrichBolt(Bolt):
    """
    Storm bolt that enriches tweets with NOAA weather data.
    
    Features:
    - Find nearest weather station within 50km
    - Query weather observations for tweet timestamp (±1 hour window)
    - Extract weather metrics (temperature, precipitation, wind)
    - Classify weather event types
    - Cache station lookups (24-hour TTL)
    - Handle API timeouts and errors gracefully
    """
    
    # Configuration
    MAX_STATION_DISTANCE_KM = 50.0
    TIME_WINDOW_HOURS = 1.0
    
    def initialize(self, conf, ctx):
        """Initialize the bolt and NOAA client."""
        self.conf = conf
        self.ctx = ctx
        
        # Initialize NOAA client
        self.noaa_client = NOAAClient()
        
        # Statistics
        self.processed = 0
        self.failed = 0
        self.enriched = 0
        self.no_station = 0
        self.no_observations = 0
        self.skipped_no_geo = 0
        
        logger.info("NOAAEnrichBolt initialized")
    
    def process(self, tup):
        """
        Process incoming tuple from GeoResolveBolt.
        
        Args:
            tup: Tuple containing geolocated tweet data
        """
        try:
            # Extract tweet data
            tweet_data = tup.values[0]
            
            # Enrich with NOAA data
            enriched_data = self._enrich_with_noaa(tweet_data)
            
            if not enriched_data:
                logger.error(f"Failed to enrich tweet {tweet_data.get('tweet_id')}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Add processing stage
            enriched_data['processing_stage'] = 'noaa_enriched'
            
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
    
    def _enrich_with_noaa(self, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Enrich tweet with NOAA weather data.
        
        Args:
            tweet_data: Tweet data with geolocation
            
        Returns:
            Enriched tweet data or None
        """
        try:
            # Check if geolocation is available
            geo_confidence = tweet_data.get('geo_confidence', 'unavailable')
            if geo_confidence == 'unavailable':
                self.skipped_no_geo += 1
                # Return tweet with null weather fields
                return self._add_null_weather_fields(tweet_data)
            
            lat = tweet_data.get('geo_lat')
            lon = tweet_data.get('geo_lon')
            
            if lat is None or lon is None:
                self.skipped_no_geo += 1
                return self._add_null_weather_fields(tweet_data)
            
            # Find nearest weather station
            station = self.noaa_client.find_nearest_station(
                lat=lat,
                lon=lon,
                max_distance_km=self.MAX_STATION_DISTANCE_KM
            )
            
            if not station:
                self.no_station += 1
                logger.debug(f"No weather station found within {self.MAX_STATION_DISTANCE_KM}km "
                           f"of ({lat:.2f}, {lon:.2f})")
                return self._add_null_weather_fields(tweet_data)
            
            # Get tweet timestamp
            created_at = tweet_data.get('created_at')
            if isinstance(created_at, str):
                # Parse if string
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            
            if not isinstance(created_at, datetime):
                logger.warning(f"Invalid created_at timestamp: {created_at}")
                return self._add_null_weather_fields(tweet_data)
            
            # Query observations within time window
            start_time = created_at - timedelta(hours=self.TIME_WINDOW_HOURS)
            end_time = created_at + timedelta(hours=self.TIME_WINDOW_HOURS)
            
            observations = self.noaa_client.query_observations(
                station_id=station['id'],
                start_time=start_time,
                end_time=end_time
            )
            
            if not observations:
                self.no_observations += 1
                logger.debug(f"No observations found for station {station['id']} "
                           f"in time window {start_time} to {end_time}")
                # Return with station info but no observations
                enriched = tweet_data.copy()
                enriched.update({
                    'weather_station_id': station['id'],
                    'weather_station_name': station['name'],
                    'weather_station_distance_km': station['distance_km'],
                    'weather_temp_c': None,
                    'weather_temp_max_c': None,
                    'weather_temp_min_c': None,
                    'weather_precip_mm': None,
                    'weather_wind_speed_ms': None,
                    'weather_event_type': None,
                    'weather_observation_time': None,
                    'noaa_match_quality': 'no_observations'
                })
                return enriched
            
            # Aggregate observations
            weather_metrics = self._aggregate_observations(observations, created_at)
            
            # Classify weather event type
            event_type = self._classify_weather_event(weather_metrics)
            
            # Compute match quality
            match_quality = self._compute_match_quality(
                observations,
                created_at,
                station['distance_km']
            )
            
            # Build enriched data
            enriched = tweet_data.copy()
            enriched.update({
                'weather_station_id': station['id'],
                'weather_station_name': station['name'],
                'weather_station_distance_km': station['distance_km'],
                'weather_temp_c': weather_metrics.get('temperature_avg_c'),
                'weather_temp_max_c': weather_metrics.get('temperature_max_c'),
                'weather_temp_min_c': weather_metrics.get('temperature_min_c'),
                'weather_precip_mm': weather_metrics.get('precipitation_mm'),
                'weather_wind_speed_ms': weather_metrics.get('wind_speed_ms'),
                'weather_event_type': event_type,
                'weather_observation_time': weather_metrics.get('closest_observation_time'),
                'noaa_match_quality': match_quality
            })
            
            self.enriched += 1
            return enriched
        
        except Exception as e:
            logger.error(f"Error enriching with NOAA data: {e}")
            return None
    
    def _add_null_weather_fields(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add null weather fields to tweet data."""
        enriched = tweet_data.copy()
        enriched.update({
            'weather_station_id': None,
            'weather_station_name': None,
            'weather_station_distance_km': None,
            'weather_temp_c': None,
            'weather_temp_max_c': None,
            'weather_temp_min_c': None,
            'weather_precip_mm': None,
            'weather_wind_speed_ms': None,
            'weather_event_type': None,
            'weather_observation_time': None,
            'noaa_match_quality': 'no_geolocation'
        })
        return enriched
    
    def _aggregate_observations(
        self,
        observations: list,
        tweet_time: datetime
    ) -> Dict[str, Any]:
        """
        Aggregate weather observations into metrics.
        
        Args:
            observations: List of observation dicts
            tweet_time: Tweet timestamp
            
        Returns:
            Dict with aggregated weather metrics
        """
        metrics = {}
        
        # Find closest observation by time
        closest_obs = None
        min_time_diff = float('inf')
        
        # Collect all values
        temps_max = []
        temps_min = []
        precip = []
        wind_speeds = []
        
        for obs in observations:
            obs_time_str = obs.get('timestamp')
            if obs_time_str:
                try:
                    obs_time = datetime.fromisoformat(obs_time_str.replace('Z', '+00:00'))
                    time_diff = abs((obs_time - tweet_time).total_seconds())
                    
                    if time_diff < min_time_diff:
                        min_time_diff = time_diff
                        closest_obs = obs
                except:
                    pass
            
            # Collect values
            if 'temperature_max_c' in obs:
                temps_max.append(obs['temperature_max_c'])
            if 'temperature_min_c' in obs:
                temps_min.append(obs['temperature_min_c'])
            if 'precipitation_mm' in obs:
                precip.append(obs['precipitation_mm'])
            if 'wind_speed_ms' in obs:
                wind_speeds.append(obs['wind_speed_ms'])
        
        # Aggregate
        if temps_max:
            metrics['temperature_max_c'] = round(max(temps_max), 1)
        if temps_min:
            metrics['temperature_min_c'] = round(min(temps_min), 1)
        if temps_max and temps_min:
            metrics['temperature_avg_c'] = round((max(temps_max) + min(temps_min)) / 2, 1)
        if precip:
            metrics['precipitation_mm'] = round(sum(precip), 1)
        if wind_speeds:
            metrics['wind_speed_ms'] = round(sum(wind_speeds) / len(wind_speeds), 1)
        
        if closest_obs:
            metrics['closest_observation_time'] = closest_obs.get('timestamp')
        
        return metrics
    
    def _classify_weather_event(self, metrics: Dict[str, Any]) -> Optional[str]:
        """
        Classify weather event type based on metrics.
        
        Args:
            metrics: Weather metrics dict
            
        Returns:
            Event type string or None
        """
        temp_max = metrics.get('temperature_max_c')
        precip = metrics.get('precipitation_mm', 0)
        wind = metrics.get('wind_speed_ms', 0)
        
        # Classification rules (simplified)
        if precip > 25:
            return 'heavy_rain'
        elif precip > 10:
            return 'rain'
        elif precip > 0:
            return 'light_rain'
        
        if wind > 20:
            return 'high_wind'
        elif wind > 10:
            return 'windy'
        
        if temp_max is not None:
            if temp_max > 35:
                return 'extreme_heat'
            elif temp_max > 30:
                return 'heat'
            elif temp_max < 0:
                return 'freezing'
            elif temp_max < 5:
                return 'cold'
        
        return 'normal'
    
    def _compute_match_quality(
        self,
        observations: list,
        tweet_time: datetime,
        station_distance: float
    ) -> str:
        """
        Compute quality of NOAA data match.
        
        Args:
            observations: List of observations
            tweet_time: Tweet timestamp
            station_distance: Distance to station in km
            
        Returns:
            Quality string: 'high', 'medium', or 'low'
        """
        if not observations:
            return 'no_observations'
        
        # Find closest observation time
        min_time_diff_minutes = float('inf')
        for obs in observations:
            obs_time_str = obs.get('timestamp')
            if obs_time_str:
                try:
                    obs_time = datetime.fromisoformat(obs_time_str.replace('Z', '+00:00'))
                    time_diff_minutes = abs((obs_time - tweet_time).total_seconds()) / 60
                    min_time_diff_minutes = min(min_time_diff_minutes, time_diff_minutes)
                except:
                    pass
        
        # Quality criteria
        if station_distance < 10 and min_time_diff_minutes < 30:
            return 'high'
        elif station_distance < 30 and min_time_diff_minutes < 60:
            return 'medium'
        else:
            return 'low'
    
    def _log_statistics(self):
        """Log enrichment statistics."""
        total = self.processed
        if total == 0:
            return
        
        enriched_pct = (self.enriched / total * 100) if total > 0 else 0
        no_station_pct = (self.no_station / total * 100) if total > 0 else 0
        no_obs_pct = (self.no_observations / total * 100) if total > 0 else 0
        no_geo_pct = (self.skipped_no_geo / total * 100) if total > 0 else 0
        
        logger.info(
            f"NOAAEnrichBolt stats: "
            f"processed={self.processed}, "
            f"failed={self.failed}, "
            f"enriched={self.enriched} ({enriched_pct:.1f}%), "
            f"no_station={self.no_station} ({no_station_pct:.1f}%), "
            f"no_observations={self.no_observations} ({no_obs_pct:.1f}%), "
            f"no_geo={self.skipped_no_geo} ({no_geo_pct:.1f}%)"
        )


if __name__ == "__main__":
    # For local testing
    from datetime import datetime
    
    bolt = NOAAEnrichBolt()
    bolt.initialize({}, {})
    
    # Test tweet with Houston coordinates
    test_tweet = {
        'tweet_id': '123',
        'text_clean': 'Heavy rain and flooding in Houston',
        'created_at': datetime(2024, 1, 15, 14, 30, 0),
        'geo_lat': 29.7604,
        'geo_lon': -95.3698,
        'geo_confidence': 'high',
        'geo_source': 'coordinates'
    }
    
    print("Testing NOAAEnrichBolt:\n")
    print(f"Tweet: {test_tweet['text_clean']}")
    print(f"Location: ({test_tweet['geo_lat']}, {test_tweet['geo_lon']})")
    print(f"Time: {test_tweet['created_at']}")
    print()
    
    enriched = bolt._enrich_with_noaa(test_tweet)
    
    if enriched:
        print("Enrichment successful!")
        print(f"  Station: {enriched.get('weather_station_name')}")
        print(f"  Distance: {enriched.get('weather_station_distance_km')} km")
        print(f"  Temperature: {enriched.get('weather_temp_c')}°C")
        print(f"  Precipitation: {enriched.get('weather_precip_mm')} mm")
        print(f"  Wind: {enriched.get('weather_wind_speed_ms')} m/s")
        print(f"  Event type: {enriched.get('weather_event_type')}")
        print(f"  Match quality: {enriched.get('noaa_match_quality')}")
    else:
        print("Enrichment failed")
