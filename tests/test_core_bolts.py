"""
Unit tests for core processing bolts: ParseBolt, DedupBolt, CleanBolt.
"""

import unittest
import json
import os
import sys
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from bolts.ParseBolt import ParseBolt
from bolts.DedupBolt import DedupBolt
from bolts.CleanBolt import CleanBolt


class TestParseBolt(unittest.TestCase):
    """Test cases for ParseBolt."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.bolt = ParseBolt()
        self.bolt.initialize({}, {})
    
    def test_parse_valid_tweet(self):
        """Test parsing a valid tweet."""
        tweet = {
            'tweet_id': '123456789',
            'text': 'Test tweet about #flood',
            'created_at': '2024-01-15T14:30:00.000Z',
            'author_id': '987654321',
            'lang': 'en',
            'entities': {
                'hashtags': [{'tag': 'flood'}],
                'mentions': [{'username': 'user1'}],
                'urls': [{'url': 'https://example.com'}]
            },
            'retweet_count': 10
        }
        
        parsed = self.bolt._parse_tweet(tweet)
        
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['tweet_id'], '123456789')
        self.assertEqual(parsed['text'], 'Test tweet about #flood')
        self.assertEqual(parsed['lang'], 'en')
        self.assertEqual(parsed['hashtags'], ['flood'])
        self.assertEqual(parsed['mentions'], ['user1'])
        self.assertEqual(parsed['hashtag_count'], 1)
        self.assertEqual(parsed['mention_count'], 1)
        self.assertEqual(parsed['url_count'], 1)
        self.assertIsInstance(parsed['created_at'], datetime)
    
    def test_parse_timestamp(self):
        """Test timestamp parsing."""
        timestamp_str = '2024-01-15T14:30:00.000Z'
        dt = self.bolt._parse_timestamp(timestamp_str)
        
        self.assertIsNotNone(dt)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(dt.year, 2024)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 15)
    
    def test_validate_required_fields(self):
        """Test required field validation."""
        # Valid tweet
        valid_tweet = {
            'tweet_id': '123',
            'text': 'Valid tweet',
            'created_at': '2024-01-15T14:30:00.000Z'
        }
        self.assertTrue(self.bolt._validate_required_fields(valid_tweet))
        
        # Missing tweet_id
        invalid_tweet = {
            'text': 'Invalid tweet',
            'created_at': '2024-01-15T14:30:00.000Z'
        }
        self.assertFalse(self.bolt._validate_required_fields(invalid_tweet))
        
        # Empty text
        empty_text_tweet = {
            'tweet_id': '123',
            'text': '   ',
            'created_at': '2024-01-15T14:30:00.000Z'
        }
        self.assertFalse(self.bolt._validate_required_fields(empty_text_tweet))
    
    def test_entity_flattening(self):
        """Test entity extraction and flattening."""
        tweet = {
            'tweet_id': '123',
            'text': 'Test',
            'created_at': '2024-01-15T14:30:00.000Z',
            'entities': {
                'hashtags': [{'tag': 'climate'}, {'tag': 'flood'}],
                'mentions': [{'username': 'user1'}, {'username': 'user2'}]
            }
        }
        
        parsed = self.bolt._parse_tweet(tweet)
        
        self.assertEqual(parsed['hashtags'], ['climate', 'flood'])
        self.assertEqual(parsed['mentions'], ['user1', 'user2'])
        self.assertEqual(parsed['hashtag_count'], 2)
        self.assertEqual(parsed['mention_count'], 2)


class TestDedupBolt(unittest.TestCase):
    """Test cases for DedupBolt."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.bolt = DedupBolt()
        self.bolt.initialize({}, {})
    
    def test_duplicate_id_detection(self):
        """Test duplicate tweet ID detection."""
        tweet_id = '123456789'
        
        # First occurrence - not duplicate
        self.assertFalse(self.bolt._is_duplicate_id(tweet_id))
        
        # Add to cache
        text_hash = self.bolt._compute_text_hash('test')
        self.bolt._add_to_cache(tweet_id, text_hash)
        
        # Second occurrence - duplicate
        self.assertTrue(self.bolt._is_duplicate_id(tweet_id))
    
    def test_duplicate_text_detection(self):
        """Test duplicate text hash detection."""
        text1 = 'This is a test tweet'
        text2 = 'This is a test tweet'  # Same text
        text3 = 'This is a different tweet'
        
        hash1 = self.bolt._compute_text_hash(text1)
        hash2 = self.bolt._compute_text_hash(text2)
        hash3 = self.bolt._compute_text_hash(text3)
        
        # Same text should produce same hash
        self.assertEqual(hash1, hash2)
        self.assertNotEqual(hash1, hash3)
        
        # First occurrence - not duplicate
        self.assertFalse(self.bolt._is_duplicate_hash(hash1))
        
        # Add to cache
        self.bolt._add_to_cache('123', hash1)
        
        # Second occurrence - duplicate
        self.assertTrue(self.bolt._is_duplicate_hash(hash2))
        
        # Different text - not duplicate
        self.assertFalse(self.bolt._is_duplicate_hash(hash3))
    
    def test_text_hash_normalization(self):
        """Test that text hashing normalizes case and whitespace."""
        text1 = 'Test Tweet'
        text2 = 'test tweet'
        text3 = '  Test Tweet  '
        
        hash1 = self.bolt._compute_text_hash(text1)
        hash2 = self.bolt._compute_text_hash(text2)
        hash3 = self.bolt._compute_text_hash(text3)
        
        # All should produce same hash after normalization
        self.assertEqual(hash1, hash2)
        self.assertEqual(hash1, hash3)
    
    def test_cache_lru_eviction(self):
        """Test LRU cache eviction when max size reached."""
        # Set small cache size for testing
        self.bolt.max_cache_size = 3
        
        # Add 4 items (should evict oldest)
        for i in range(4):
            tweet_id = f'tweet_{i}'
            text_hash = self.bolt._compute_text_hash(f'text_{i}')
            self.bolt._add_to_cache(tweet_id, text_hash)
        
        # Cache should only have 3 items
        self.assertEqual(len(self.bolt.seen_tweet_ids), 3)
        self.assertEqual(len(self.bolt.seen_text_hashes), 3)
        
        # First item should be evicted
        self.assertNotIn('tweet_0', self.bolt.seen_tweet_ids)
        
        # Last 3 items should be present
        self.assertIn('tweet_1', self.bolt.seen_tweet_ids)
        self.assertIn('tweet_2', self.bolt.seen_tweet_ids)
        self.assertIn('tweet_3', self.bolt.seen_tweet_ids)


class TestCleanBolt(unittest.TestCase):
    """Test cases for CleanBolt."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.bolt = CleanBolt()
        self.bolt.initialize({}, {})
    
    def test_emoji_processing(self):
        """Test emoji extraction and replacement."""
        text = "Great weather today! 😊🌞"
        
        emoji_data = self.bolt._process_emojis(text)
        
        # Check emoji count
        self.assertEqual(emoji_data['emoji_count'], 2)
        
        # Check sentiment score (both positive emojis)
        self.assertGreater(emoji_data['emoji_sentiment_score'], 0)
        
        # Check text has emoji tokens
        self.assertIn(':', emoji_data['text'])
    
    def test_hashtag_decomposition(self):
        """Test camelCase hashtag decomposition."""
        # Test camelCase
        self.assertEqual(
            self.bolt._decompose_camelcase('ClimateChange'),
            'climate change'
        )
        
        # Test with numbers
        self.assertEqual(
            self.bolt._decompose_camelcase('Hurricane2024'),
            'hurricane 2024'
        )
        
        # Test already lowercase
        self.assertEqual(
            self.bolt._decompose_camelcase('flood'),
            'flood'
        )
    
    def test_url_removal(self):
        """Test URL removal."""
        text = "Check this out https://example.com and www.test.com"
        
        cleaned = self.bolt._remove_urls(text)
        
        self.assertNotIn('https://example.com', cleaned)
        self.assertNotIn('www.test.com', cleaned)
        self.assertIn('Check this out', cleaned)
    
    def test_contraction_expansion(self):
        """Test contraction expansion."""
        text = "Don't worry, we'll be fine"
        
        expanded = self.bolt._expand_contractions(text)
        
        # Should expand contractions (if library available)
        if self.bolt._expand_contractions(text) != text:
            self.assertIn('do not', expanded.lower())
            self.assertIn('we will', expanded.lower())
    
    def test_whitespace_normalization(self):
        """Test whitespace normalization."""
        text = "  Multiple    spaces   here  "
        
        normalized = self.bolt._normalize_whitespace(text)
        
        # Should have single spaces and no leading/trailing whitespace
        self.assertEqual(normalized, "Multiple spaces here")
        self.assertNotIn('  ', normalized)
    
    def test_full_cleaning_pipeline(self):
        """Test complete cleaning pipeline."""
        tweet = {
            'tweet_id': '123',
            'text': "Don't miss this! 😊 #ClimateChange https://example.com",
            'hashtags': ['ClimateChange'],
            'hashtag_count': 1,
            'mention_count': 0,
            'url_count': 1
        }
        
        cleaned = self.bolt._clean_tweet(tweet)
        
        self.assertIsNotNone(cleaned)
        
        # Check cleaned text
        text_clean = cleaned['text_clean']
        
        # URL should be removed
        self.assertNotIn('https://example.com', text_clean)
        
        # Hashtag should be decomposed
        self.assertIn('climate change', text_clean.lower())
        
        # Features should be extracted
        self.assertEqual(cleaned['hashtag_count'], 1)
        self.assertEqual(cleaned['emoji_count'], 1)
        self.assertGreater(cleaned['emoji_sentiment_score'], 0)
        self.assertGreater(cleaned['text_length'], 0)
        self.assertGreater(cleaned['word_count'], 0)
    
    def test_feature_extraction(self):
        """Test feature extraction."""
        tweet = {
            'tweet_id': '123',
            'text': 'Test tweet with #hashtag @mention https://url.com',
            'hashtags': ['hashtag'],
            'hashtag_count': 1,
            'mention_count': 1,
            'url_count': 1
        }
        
        cleaned = self.bolt._clean_tweet(tweet)
        
        # Check all features are present
        self.assertIn('hashtag_count', cleaned)
        self.assertIn('mention_count', cleaned)
        self.assertIn('url_count', cleaned)
        self.assertIn('emoji_count', cleaned)
        self.assertIn('text_length', cleaned)
        self.assertIn('word_count', cleaned)
        self.assertIn('has_media', cleaned)
        self.assertIn('avg_word_length', cleaned)
        
        # Check feature values
        self.assertEqual(cleaned['hashtag_count'], 1)
        self.assertEqual(cleaned['mention_count'], 1)
        self.assertEqual(cleaned['url_count'], 1)
        self.assertTrue(cleaned['has_media'])


if __name__ == '__main__':
    unittest.main()
