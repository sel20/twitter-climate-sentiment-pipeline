"""
TwitterSpout - Ingests tweets from Twitter API v2 or local JSON files.

This spout connects to Twitter API v2, polls for climate-related tweets,
and emits tuples into the Storm topology. Implements rate limit handling
with exponential backoff and fallback to local JSON files.
"""

import json
import time
import os
import logging
from typing import Optional, Dict, Any, List
import requests
from datetime import datetime, timedelta

# Storm imports (for production)
try:
    from streamparse.spout import Spout
except ImportError:
    # Fallback for local development without streamparse
    class Spout:
        def initialize(self, stormconf, context):
            pass
        def next_tuple(self):
            pass
        def ack(self, tup_id):
            pass
        def fail(self, tup_id):
            pass

logger = logging.getLogger(__name__)


class TwitterSpout(Spout):
    """
    Storm spout that ingests tweets from Twitter API v2.
    
    Features:
    - Polls Twitter API v2 /tweets/search/recent endpoint
    - Climate-related keyword filtering
    - Rate limit handling with exponential backoff
    - Fallback to local JSON file for development
    - Bearer token authentication
    """
    
    # Twitter API v2 endpoint
    TWITTER_API_URL = "https://api.twitter.com/2/tweets/search/recent"
    
    # Climate-related search query
    SEARCH_QUERY = "(storm OR cyclone OR flood OR heatwave OR wildfire OR #climatechange OR #climate OR #weather) -is:retweet"
    
    # Rate limiting
    MAX_BACKOFF_SECONDS = 300
    INITIAL_BACKOFF_SECONDS = 60
    
    def initialize(self, stormconf, context):
        """Initialize the spout with configuration."""
        self.conf = stormconf
        
        # Load configuration from environment
        self.bearer_token = os.getenv('TWITTER_BEARER_TOKEN', '')
        self.poll_interval = int(os.getenv('TWITTER_POLL_INTERVAL_SECONDS', '30'))
        self.use_local_file = os.getenv('USE_LOCAL_FILE', 'false').lower() == 'true'
        self.local_file_path = os.getenv('LOCAL_SAMPLE_FILE', 'data/sample_tweets.json')
        
        # State management
        self.last_poll_time = 0
        self.backoff_seconds = 0
        self.tweet_buffer = []
        self.buffer_index = 0
        self.last_tweet_id = None  # For pagination
        
        # Statistics
        self.total_emitted = 0
        self.api_calls = 0
        self.rate_limit_hits = 0
        
        logger.info(f"TwitterSpout initialized")
        logger.info(f"  Use local file: {self.use_local_file}")
        logger.info(f"  Poll interval: {self.poll_interval}s")
        
        # If using local file, load it once
        if self.use_local_file:
            self._load_local_file()
    
    def next_tuple(self):
        """
        Emit the next tweet tuple.
        
        This method is called repeatedly by Storm. It either:
        1. Emits a tweet from the buffer
        2. Polls Twitter API for new tweets
        3. Waits if in backoff period
        """
        current_time = time.time()
        
        # If we have tweets in buffer, emit them
        if self.tweet_buffer and self.buffer_index < len(self.tweet_buffer):
            tweet = self.tweet_buffer[self.buffer_index]
            self.buffer_index += 1
            
            # Emit tuple
            tuple_data = self._create_tuple(tweet)
            if tuple_data:
                self.emit([tuple_data], tup_id=tweet.get('id'))
                self.total_emitted += 1
                
                if self.total_emitted % 100 == 0:
                    logger.info(f"Emitted {self.total_emitted} tweets total")
            
            return
        
        # Check if we're in backoff period
        if self.backoff_seconds > 0:
            time_since_last_poll = current_time - self.last_poll_time
            if time_since_last_poll < self.backoff_seconds:
                time.sleep(1)  # Sleep briefly
                return
            else:
                # Backoff period over
                logger.info(f"Backoff period ended ({self.backoff_seconds}s)")
                self.backoff_seconds = 0
        
        # Check if it's time to poll
        time_since_last_poll = current_time - self.last_poll_time
        if time_since_last_poll < self.poll_interval:
            time.sleep(1)
            return
        
        # Poll for new tweets
        self._poll_tweets()
        self.last_poll_time = current_time
    
    def _poll_tweets(self):
        """Poll Twitter API or local file for new tweets."""
        if self.use_local_file:
            self._load_local_file()
        else:
            self._poll_twitter_api()
    
    def _poll_twitter_api(self):
        """Poll Twitter API v2 for recent tweets."""
        if not self.bearer_token:
            logger.error("Twitter bearer token not configured, switching to local file")
            self.use_local_file = True
            self._load_local_file()
            return
        
        try:
            # Build request parameters
            params = {
                'query': self.SEARCH_QUERY,
                'max_results': 100,  # Max allowed by API
                'tweet.fields': 'created_at,author_id,lang,geo,entities,public_metrics',
                'expansions': 'geo.place_id',
                'place.fields': 'full_name,geo'
            }
            
            # Add pagination if we have a last tweet ID
            if self.last_tweet_id:
                params['since_id'] = self.last_tweet_id
            
            # Make API request
            headers = {
                'Authorization': f'Bearer {self.bearer_token}'
            }
            
            logger.info(f"Polling Twitter API (call #{self.api_calls + 1})")
            response = requests.get(
                self.TWITTER_API_URL,
                headers=headers,
                params=params,
                timeout=30
            )
            
            self.api_calls += 1
            
            # Handle rate limiting
            if response.status_code == 429:
                self._handle_rate_limit(response)
                return
            
            # Handle authentication errors
            if response.status_code == 401:
                logger.error("Twitter API authentication failed, switching to local file")
                self.use_local_file = True
                self._load_local_file()
                return
            
            # Handle other errors
            if response.status_code != 200:
                logger.warning(f"Twitter API error: {response.status_code} - {response.text}")
                self._apply_backoff()
                return
            
            # Parse response
            data = response.json()
            tweets = data.get('data', [])
            includes = data.get('includes', {})
            
            if not tweets:
                logger.info("No new tweets found")
                return
            
            # Store place data for lookup
            places = {place['id']: place for place in includes.get('places', [])}
            
            # Process tweets and add place data
            for tweet in tweets:
                if 'geo' in tweet and 'place_id' in tweet['geo']:
                    place_id = tweet['geo']['place_id']
                    if place_id in places:
                        tweet['place'] = places[place_id]
            
            # Update buffer
            self.tweet_buffer = tweets
            self.buffer_index = 0
            
            # Update last tweet ID for pagination
            if tweets:
                self.last_tweet_id = tweets[0]['id']
            
            logger.info(f"Fetched {len(tweets)} new tweets")
            
            # Reset backoff on success
            self.backoff_seconds = 0
            
        except requests.exceptions.Timeout:
            logger.warning("Twitter API request timed out")
            self._apply_backoff()
        except requests.exceptions.RequestException as e:
            logger.error(f"Twitter API request failed: {e}")
            self._apply_backoff()
        except Exception as e:
            logger.error(f"Unexpected error polling Twitter API: {e}")
            self._apply_backoff()
    
    def _handle_rate_limit(self, response):
        """Handle Twitter API rate limit (HTTP 429)."""
        self.rate_limit_hits += 1
        
        # Try to get reset time from headers
        reset_time = response.headers.get('x-rate-limit-reset')
        if reset_time:
            reset_timestamp = int(reset_time)
            current_timestamp = int(time.time())
            wait_seconds = max(reset_timestamp - current_timestamp, self.INITIAL_BACKOFF_SECONDS)
        else:
            # Use exponential backoff
            wait_seconds = min(
                self.INITIAL_BACKOFF_SECONDS * (2 ** (self.rate_limit_hits - 1)),
                self.MAX_BACKOFF_SECONDS
            )
        
        self.backoff_seconds = wait_seconds
        logger.warning(f"Rate limit hit (#{self.rate_limit_hits}), backing off for {wait_seconds}s")
    
    def _apply_backoff(self):
        """Apply exponential backoff after errors."""
        if self.backoff_seconds == 0:
            self.backoff_seconds = self.INITIAL_BACKOFF_SECONDS
        else:
            self.backoff_seconds = min(self.backoff_seconds * 2, self.MAX_BACKOFF_SECONDS)
        
        logger.info(f"Applying backoff: {self.backoff_seconds}s")
    
    def _load_local_file(self):
        """Load tweets from local JSON file."""
        try:
            with open(self.local_file_path, 'r', encoding='utf-8') as f:
                tweets = json.load(f)
            
            self.tweet_buffer = tweets if isinstance(tweets, list) else [tweets]
            self.buffer_index = 0
            
            logger.info(f"Loaded {len(self.tweet_buffer)} tweets from {self.local_file_path}")
            
        except FileNotFoundError:
            logger.error(f"Local file not found: {self.local_file_path}")
            self.tweet_buffer = []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in local file: {e}")
            self.tweet_buffer = []
        except Exception as e:
            logger.error(f"Error loading local file: {e}")
            self.tweet_buffer = []
    
    def _create_tuple(self, tweet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a structured tuple from raw tweet data.
        
        Args:
            tweet: Raw tweet dict from API or file
            
        Returns:
            Structured tuple dict or None if invalid
        """
        try:
            # Extract coordinates
            coordinates = None
            if 'geo' in tweet and tweet['geo']:
                if 'coordinates' in tweet['geo']:
                    coords = tweet['geo']['coordinates']
                    if coords.get('type') == 'Point':
                        lon, lat = coords['coordinates']
                        coordinates = {'lat': lat, 'lon': lon}
            
            # Extract place info
            place = tweet.get('place')
            
            # Extract user location (not in v2 API, would need user expansion)
            user_location = None
            
            # Check if retweet
            is_retweet = tweet.get('text', '').startswith('RT @')
            
            # Extract entities
            entities = tweet.get('entities', {})
            
            # Create tuple
            tuple_data = {
                'tweet_id': tweet['id'],
                'raw_json': json.dumps(tweet),
                'created_at': tweet.get('created_at'),
                'text': tweet.get('text', ''),
                'author_id': tweet.get('author_id'),
                'lang': tweet.get('lang', 'unknown'),
                'coordinates': coordinates,
                'place': place,
                'user_location': user_location,
                'retweet_status': is_retweet,
                'retweet_count': tweet.get('public_metrics', {}).get('retweet_count', 0),
                'entities': entities
            }
            
            return tuple_data
            
        except Exception as e:
            logger.error(f"Error creating tuple from tweet: {e}")
            return None
    
    def ack(self, tup_id):
        """Acknowledge successful processing of tuple."""
        pass
    
    def fail(self, tup_id):
        """Handle failed tuple processing."""
        logger.warning(f"Tuple failed: {tup_id}")
