"""
ParseBolt - Parses raw JSON into structured tuple fields.

This bolt validates and extracts fields from raw tweet JSON,
converting them into properly typed structured data.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

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


class ParseBolt(Bolt):
    """
    Storm bolt that parses raw tweet JSON into structured fields.
    
    Features:
    - JSON validation and parsing
    - Type conversion (timestamps, IDs)
    - Field extraction with null handling
    - Entity flattening (hashtags, mentions)
    - Error handling with tuple failure
    """
    
    def initialize(self, conf, ctx):
        """Initialize the bolt."""
        self.conf = conf
        self.ctx = ctx
        
        # Statistics
        self.processed = 0
        self.failed = 0
        
        logger.info("ParseBolt initialized")
    
    def process(self, tup):
        """
        Process incoming tuple from TwitterSpout.
        
        Args:
            tup: Tuple containing tweet data dict
        """
        try:
            # Extract tweet data from tuple
            tweet_data = tup.values[0]
            
            # Validate that we have a dict
            if not isinstance(tweet_data, dict):
                logger.error(f"Invalid tuple data type: {type(tweet_data)}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Validate required fields
            if not self._validate_required_fields(tweet_data):
                logger.warning(f"Missing required fields in tweet {tweet_data.get('tweet_id')}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Parse and convert fields
            parsed_data = self._parse_tweet(tweet_data)
            
            if not parsed_data:
                logger.error(f"Failed to parse tweet {tweet_data.get('tweet_id')}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Emit parsed tuple
            self.emit([parsed_data], anchors=[tup])
            self.ack(tup)
            
            self.processed += 1
            if self.processed % 1000 == 0:
                logger.info(f"ParseBolt: processed {self.processed}, failed {self.failed}")
            
        except Exception as e:
            logger.error(f"Error processing tuple: {e}", exc_info=True)
            self.fail(tup)
            self.failed += 1
    
    def _validate_required_fields(self, tweet_data: Dict[str, Any]) -> bool:
        """
        Validate that required fields are present.
        
        Args:
            tweet_data: Tweet data dict
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['tweet_id', 'text', 'created_at']
        
        for field in required_fields:
            if field not in tweet_data or tweet_data[field] is None:
                return False
        
        # Check that text is not empty
        if not tweet_data['text'].strip():
            return False
        
        return True
    
    def _parse_tweet(self, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse tweet data into structured format.
        
        Args:
            tweet_data: Raw tweet data dict
            
        Returns:
            Parsed tweet dict or None if parsing fails
        """
        try:
            # Convert timestamp to datetime object
            created_at = self._parse_timestamp(tweet_data['created_at'])
            if not created_at:
                logger.warning(f"Invalid timestamp: {tweet_data['created_at']}")
                return None
            
            # Flatten entities
            entities = tweet_data.get('entities', {})
            hashtags = [h.get('tag', '') for h in entities.get('hashtags', [])]
            mentions = [m.get('username', '') for m in entities.get('mentions', [])]
            urls = [u.get('url', '') for u in entities.get('urls', [])]
            
            # Build parsed data structure
            parsed = {
                # Identity fields
                'tweet_id': str(tweet_data['tweet_id']),
                'author_id': str(tweet_data.get('author_id', 'unknown')),
                'created_at': created_at,
                'created_at_str': tweet_data['created_at'],
                
                # Text fields
                'text': tweet_data['text'],
                'lang': tweet_data.get('lang', 'unknown'),
                
                # Geolocation fields (pass through)
                'coordinates': tweet_data.get('coordinates'),
                'place': tweet_data.get('place'),
                'user_location': tweet_data.get('user_location'),
                
                # Metadata
                'retweet_status': tweet_data.get('retweet_status', False),
                'retweet_count': int(tweet_data.get('retweet_count', 0)),
                
                # Flattened entities
                'hashtags': hashtags,
                'mentions': mentions,
                'urls': urls,
                'hashtag_count': len(hashtags),
                'mention_count': len(mentions),
                'url_count': len(urls),
                
                # Raw JSON for audit
                'raw_json': tweet_data.get('raw_json', json.dumps(tweet_data)),
                
                # Processing metadata
                'processing_stage': 'parsed',
                'parse_timestamp': datetime.utcnow().isoformat()
            }
            
            return parsed
            
        except Exception as e:
            logger.error(f"Error parsing tweet: {e}", exc_info=True)
            return None
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse ISO 8601 timestamp string to datetime object.
        
        Args:
            timestamp_str: ISO 8601 timestamp string
            
        Returns:
            datetime object or None if parsing fails
        """
        try:
            # Handle Twitter's ISO 8601 format: 2024-01-15T14:30:00.000Z
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            
            # Try parsing with fromisoformat (Python 3.7+)
            try:
                return datetime.fromisoformat(timestamp_str)
            except:
                # Fallback to strptime
                return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f%z')
        
        except Exception as e:
            logger.error(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None


if __name__ == "__main__":
    # For local testing
    import sys
    
    # Test data
    test_tweet = {
        'tweet_id': '123456789',
        'text': 'Test tweet about #flood',
        'created_at': '2024-01-15T14:30:00.000Z',
        'author_id': '987654321',
        'lang': 'en',
        'coordinates': {'lat': 29.7604, 'lon': -95.3698},
        'entities': {
            'hashtags': [{'tag': 'flood'}],
            'mentions': [{'username': 'user1'}]
        },
        'retweet_count': 10
    }
    
    bolt = ParseBolt()
    bolt.initialize({}, {})
    
    parsed = bolt._parse_tweet(test_tweet)
    if parsed:
        print("✓ Parsing successful")
        print(f"  Tweet ID: {parsed['tweet_id']}")
        print(f"  Created at: {parsed['created_at']}")
        print(f"  Hashtags: {parsed['hashtags']}")
        print(f"  Mentions: {parsed['mentions']}")
    else:
        print("✗ Parsing failed")
