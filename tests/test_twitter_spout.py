"""
Unit tests for TwitterSpout.
"""

import unittest
import json
import os
from unittest.mock import Mock, patch, MagicMock
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from spouts.TwitterSpout import TwitterSpout


class TestTwitterSpout(unittest.TestCase):
    """Test cases for TwitterSpout."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.spout = TwitterSpout()
        
        # Mock configuration
        self.mock_conf = {}
        self.mock_context = {}
        
        # Set environment variables for testing
        os.environ['TWITTER_BEARER_TOKEN'] = 'test_token'
        os.environ['TWITTER_POLL_INTERVAL_SECONDS'] = '1'
        os.environ['USE_LOCAL_FILE'] = 'true'
        os.environ['LOCAL_SAMPLE_FILE'] = 'tests/fixtures/test_tweets.json'
    
    def tearDown(self):
        """Clean up after tests."""
        # Clean up environment variables
        for key in ['TWITTER_BEARER_TOKEN', 'TWITTER_POLL_INTERVAL_SECONDS', 
                    'USE_LOCAL_FILE', 'LOCAL_SAMPLE_FILE']:
            if key in os.environ:
                del os.environ[key]
    
    def test_initialization(self):
        """Test spout initialization."""
        self.spout.initialize(self.mock_conf, self.mock_context)
        
        self.assertEqual(self.spout.bearer_token, 'test_token')
        self.assertEqual(self.spout.poll_interval, 1)
        self.assertTrue(self.spout.use_local_file)
        self.assertEqual(self.spout.total_emitted, 0)
    
    def test_create_tuple_with_coordinates(self):
        """Test tuple creation with coordinate data."""
        tweet = {
            'id': '123456789',
            'text': 'Test tweet about #flood',
            'created_at': '2024-01-15T12:00:00.000Z',
            'author_id': '987654321',
            'lang': 'en',
            'geo': {
                'coordinates': {
                    'type': 'Point',
                    'coordinates': [-95.3698, 29.7604]
                }
            },
            'entities': {
                'hashtags': [{'tag': 'flood'}]
            },
            'public_metrics': {
                'retweet_count': 10
            }
        }
        
        self.spout.initialize(self.mock_conf, self.mock_context)
        tuple_data = self.spout._create_tuple(tweet)
        
        self.assertIsNotNone(tuple_data)
        self.assertEqual(tuple_data['tweet_id'], '123456789')
        self.assertEqual(tuple_data['text'], 'Test tweet about #flood')
        self.assertEqual(tuple_data['lang'], 'en')
        self.assertIsNotNone(tuple_data['coordinates'])
        self.assertEqual(tuple_data['coordinates']['lat'], 29.7604)
        self.assertEqual(tuple_data['coordinates']['lon'], -95.3698)
        self.assertFalse(tuple_data['retweet_status'])
    
    def test_create_tuple_with_place(self):
        """Test tuple creation with place data."""
        tweet = {
            'id': '123456790',
            'text': 'Storm warning!',
            'created_at': '2024-01-15T12:00:00.000Z',
            'author_id': '987654322',
            'lang': 'en',
            'place': {
                'id': 'place123',
                'full_name': 'Miami, FL',
                'geo': {
                    'type': 'Feature',
                    'bbox': [-80.3182, 25.7090, -80.1918, 25.8557]
                }
            },
            'entities': {},
            'public_metrics': {
                'retweet_count': 5
            }
        }
        
        self.spout.initialize(self.mock_conf, self.mock_context)
        tuple_data = self.spout._create_tuple(tweet)
        
        self.assertIsNotNone(tuple_data)
        self.assertIsNotNone(tuple_data['place'])
        self.assertEqual(tuple_data['place']['full_name'], 'Miami, FL')
    
    def test_create_tuple_retweet_detection(self):
        """Test retweet detection."""
        tweet = {
            'id': '123456791',
            'text': 'RT @user: This is a retweet',
            'created_at': '2024-01-15T12:00:00.000Z',
            'author_id': '987654323',
            'lang': 'en',
            'entities': {},
            'public_metrics': {
                'retweet_count': 0
            }
        }
        
        self.spout.initialize(self.mock_conf, self.mock_context)
        tuple_data = self.spout._create_tuple(tweet)
        
        self.assertIsNotNone(tuple_data)
        self.assertTrue(tuple_data['retweet_status'])
    
    @patch('spouts.TwitterSpout.requests.get')
    def test_rate_limit_handling(self, mock_get):
        """Test rate limit handling with exponential backoff."""
        # Mock rate limit response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {'x-rate-limit-reset': str(int(1000000000))}
        mock_get.return_value = mock_response
        
        # Set to use API
        os.environ['USE_LOCAL_FILE'] = 'false'
        self.spout.initialize(self.mock_conf, self.mock_context)
        
        # Poll API
        self.spout._poll_twitter_api()
        
        # Check that backoff was applied
        self.assertGreater(self.spout.backoff_seconds, 0)
        self.assertEqual(self.spout.rate_limit_hits, 1)
    
    @patch('spouts.TwitterSpout.requests.get')
    def test_api_success(self, mock_get):
        """Test successful API response."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [
                {
                    'id': '123',
                    'text': 'Test tweet',
                    'created_at': '2024-01-15T12:00:00.000Z',
                    'author_id': '456',
                    'lang': 'en'
                }
            ],
            'includes': {}
        }
        mock_get.return_value = mock_response
        
        # Set to use API
        os.environ['USE_LOCAL_FILE'] = 'false'
        self.spout.initialize(self.mock_conf, self.mock_context)
        
        # Poll API
        self.spout._poll_twitter_api()
        
        # Check that tweets were buffered
        self.assertEqual(len(self.spout.tweet_buffer), 1)
        self.assertEqual(self.spout.tweet_buffer[0]['id'], '123')
    
    @patch('spouts.TwitterSpout.requests.get')
    def test_api_auth_failure_fallback(self, mock_get):
        """Test fallback to local file on auth failure."""
        # Mock auth failure
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        # Set to use API initially
        os.environ['USE_LOCAL_FILE'] = 'false'
        self.spout.initialize(self.mock_conf, self.mock_context)
        
        # Poll API
        self.spout._poll_twitter_api()
        
        # Check that it switched to local file mode
        self.assertTrue(self.spout.use_local_file)
    
    def test_tuple_schema_validation(self):
        """Test that emitted tuples have correct schema."""
        tweet = {
            'id': '123456789',
            'text': 'Complete tweet',
            'created_at': '2024-01-15T12:00:00.000Z',
            'author_id': '987654321',
            'lang': 'en',
            'entities': {},
            'public_metrics': {'retweet_count': 0}
        }
        
        self.spout.initialize(self.mock_conf, self.mock_context)
        tuple_data = self.spout._create_tuple(tweet)
        
        # Check all required fields are present
        required_fields = [
            'tweet_id', 'raw_json', 'created_at', 'text', 'author_id',
            'lang', 'coordinates', 'place', 'user_location',
            'retweet_status', 'retweet_count', 'entities'
        ]
        
        for field in required_fields:
            self.assertIn(field, tuple_data, f"Missing field: {field}")
        
        # Check raw_json is valid JSON
        self.assertIsInstance(tuple_data['raw_json'], str)
        json.loads(tuple_data['raw_json'])  # Should not raise


if __name__ == '__main__':
    unittest.main()
