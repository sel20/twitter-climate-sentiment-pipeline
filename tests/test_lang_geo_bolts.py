"""
Unit tests for LangDetectBolt and GeoResolveBolt.
"""

import unittest
import os
import sys
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from bolts.LangDetectBolt import LangDetectBolt
from bolts.GeoResolveBolt import GeoResolveBolt


class TestLangDetectBolt(unittest.TestCase):
    """Test cases for LangDetectBolt."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.bolt = LangDetectBolt()
        self.bolt.initialize({}, {})
    
    def test_detect_language_english(self):
        """Test language detection for English text."""
        tweet = {
            'tweet_id': '1',
            'text_clean': 'This is a test tweet in English about climate change and weather patterns',
            'lang': 'en'
        }
        
        result = self.bolt._detect_language(tweet)
        
        self.assertIsNotNone(result)
        self.assertIn('detected_lang', result)
        self.assertIn('lang_confidence', result)
        self.assertIn('lang_method', result)
        
        # Should detect English
        self.assertEqual(result['detected_lang'], 'en')
    
    def test_detect_language_spanish(self):
        """Test language detection for Spanish text."""
        tweet = {
            'tweet_id': '2',
            'text_clean': 'Este es un tweet de prueba en español sobre el cambio climático',
            'lang': 'es'
        }
        
        result = self.bolt._detect_language(tweet)
        
        self.assertIsNotNone(result)
        # Should detect Spanish (if model available)
        if result['lang_method'] != 'twitter_fallback':
            self.assertEqual(result['detected_lang'], 'es')
    
    def test_short_text_fallback(self):
        """Test fallback to Twitter lang for short text."""
        tweet = {
            'tweet_id': '3',
            'text_clean': 'Short',
            'lang': 'en'
        }
        
        result = self.bolt._detect_language(tweet)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['detected_lang'], 'en')
        self.assertEqual(result['lang_method'], 'twitter_short_text')
    
    def test_confidence_threshold(self):
        """Test confidence threshold handling."""
        tweet = {
            'tweet_id': '4',
            'text_clean': 'This is a reasonable length tweet for language detection testing',
            'lang': 'en'
        }
        
        result = self.bolt._detect_language(tweet)
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result['lang_confidence'], float)
        self.assertGreaterEqual(result['lang_confidence'], 0.0)
        self.assertLessEqual(result['lang_confidence'], 1.0)
    
    def test_empty_text_handling(self):
        """Test handling of empty text."""
        tweet = {
            'tweet_id': '5',
            'text_clean': '',
            'lang': 'en'
        }
        
        result = self.bolt._detect_language(tweet)
        
        self.assertIsNotNone(result)
        # Should fall back to Twitter lang
        self.assertEqual(result['detected_lang'], 'en')


class TestGeoResolveBolt(unittest.TestCase):
    """Test cases for GeoResolveBolt."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.bolt = GeoResolveBolt()
        self.bolt.initialize({}, {})
    
    def test_coordinates_high_confidence(self):
        """Test geolocation from direct coordinates."""
        tweet = {
            'tweet_id': '1',
            'coordinates': {'lat': 29.7604, 'lon': -95.3698},
            'place': None,
            'user_location': None
        }
        
        result = self.bolt._resolve_geolocation(tweet)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['geo_lat'], 29.7604)
        self.assertEqual(result['geo_lon'], -95.3698)
        self.assertEqual(result['geo_confidence'], 'high')
        self.assertEqual(result['geo_source'], 'coordinates')
    
    def test_place_centroid_medium_confidence(self):
        """Test geolocation from place bounding box."""
        tweet = {
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
        }
        
        result = self.bolt._resolve_geolocation(tweet)
        
        self.assertIsNotNone(result)
        self.assertIsNotNone(result['geo_lat'])
        self.assertIsNotNone(result['geo_lon'])
        self.assertEqual(result['geo_confidence'], 'medium')
        self.assertEqual(result['geo_source'], 'place_centroid')
        self.assertEqual(result['place_name'], 'Miami, FL')
        
        # Check centroid is within bounding box
        self.assertGreater(result['geo_lat'], 25.7090)
        self.assertLess(result['geo_lat'], 25.8557)
        self.assertGreater(result['geo_lon'], -80.3182)
        self.assertLess(result['geo_lon'], -80.1918)
    
    def test_compute_place_centroid(self):
        """Test place centroid computation."""
        place = {
            'full_name': 'Test Place',
            'geo': {
                'bbox': [-80.0, 25.0, -79.0, 26.0]
            }
        }
        
        centroid = self.bolt._compute_place_centroid(place)
        
        self.assertIsNotNone(centroid)
        self.assertEqual(centroid[0], 25.5)  # lat
        self.assertEqual(centroid[1], -79.5)  # lon
    
    def test_compute_place_centroid_coordinates_format(self):
        """Test place centroid with coordinates array format."""
        place = {
            'full_name': 'Test Place',
            'geo': {
                'coordinates': [
                    [[-80.0, 25.0], [-79.0, 25.0], [-79.0, 26.0], [-80.0, 26.0]]
                ]
            }
        }
        
        centroid = self.bolt._compute_place_centroid(place)
        
        self.assertIsNotNone(centroid)
        # Should be average of all points
        self.assertAlmostEqual(centroid[0], 25.5, places=1)  # lat
        self.assertAlmostEqual(centroid[1], -79.5, places=1)  # lon
    
    @patch('bolts.GeoResolveBolt.Nominatim')
    def test_geocode_location_low_confidence(self, mock_nominatim_class):
        """Test geocoding user location string."""
        # Mock geocoder
        mock_geocoder = Mock()
        mock_location = Mock()
        mock_location.latitude = 40.7128
        mock_location.longitude = -74.0060
        mock_geocoder.geocode.return_value = mock_location
        mock_nominatim_class.return_value = mock_geocoder
        
        # Reinitialize bolt with mocked geocoder
        self.bolt.geocoder = mock_geocoder
        
        tweet = {
            'tweet_id': '3',
            'coordinates': None,
            'place': None,
            'user_location': 'New York, NY'
        }
        
        result = self.bolt._resolve_geolocation(tweet)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['geo_lat'], 40.7128)
        self.assertEqual(result['geo_lon'], -74.0060)
        self.assertEqual(result['geo_confidence'], 'low')
        self.assertEqual(result['geo_source'], 'user_location_geocoded')
        self.assertEqual(result['user_location_string'], 'New York, NY')
    
    def test_unavailable_geolocation(self):
        """Test handling of tweets with no geolocation data."""
        tweet = {
            'tweet_id': '4',
            'coordinates': None,
            'place': None,
            'user_location': None
        }
        
        result = self.bolt._resolve_geolocation(tweet)
        
        self.assertIsNotNone(result)
        self.assertIsNone(result['geo_lat'])
        self.assertIsNone(result['geo_lon'])
        self.assertEqual(result['geo_confidence'], 'unavailable')
        self.assertEqual(result['geo_source'], 'unavailable')
    
    def test_priority_order(self):
        """Test that coordinates take priority over place and user location."""
        tweet = {
            'tweet_id': '5',
            'coordinates': {'lat': 29.7604, 'lon': -95.3698},
            'place': {
                'full_name': 'Miami, FL',
                'geo': {'bbox': [-80.3182, 25.7090, -80.1918, 25.8557]}
            },
            'user_location': 'New York, NY'
        }
        
        result = self.bolt._resolve_geolocation(tweet)
        
        # Should use coordinates (highest priority)
        self.assertEqual(result['geo_confidence'], 'high')
        self.assertEqual(result['geo_source'], 'coordinates')
        self.assertEqual(result['geo_lat'], 29.7604)
    
    def test_invalid_coordinates_fallback(self):
        """Test fallback when coordinates are invalid."""
        tweet = {
            'tweet_id': '6',
            'coordinates': {'lat': None, 'lon': None},
            'place': {
                'full_name': 'Miami, FL',
                'geo': {'bbox': [-80.3182, 25.7090, -80.1918, 25.8557]}
            },
            'user_location': None
        }
        
        result = self.bolt._resolve_geolocation(tweet)
        
        # Should fall back to place
        self.assertEqual(result['geo_confidence'], 'medium')
        self.assertEqual(result['geo_source'], 'place_centroid')
    
    def test_empty_user_location_handling(self):
        """Test handling of empty user location strings."""
        tweet = {
            'tweet_id': '7',
            'coordinates': None,
            'place': None,
            'user_location': '   '  # Whitespace only
        }
        
        result = self.bolt._resolve_geolocation(tweet)
        
        # Should be unavailable (empty string after strip)
        self.assertEqual(result['geo_confidence'], 'unavailable')


if __name__ == '__main__':
    unittest.main()
