"""
DedupBolt - Removes duplicate tweets and retweets.

This bolt maintains a sliding window cache of tweet IDs and text hashes
to detect and filter out duplicates and retweets.
"""

import hashlib
import logging
from collections import OrderedDict
from typing import Dict, Any

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


class DedupBolt(Bolt):
    """
    Storm bolt that removes duplicate tweets and retweets.
    
    Features:
    - Retweet detection and filtering
    - Tweet ID deduplication
    - Text hash deduplication (for near-duplicates)
    - Sliding window cache (LRU with max size)
    - Deduplication metrics logging
    """
    
    def initialize(self, conf, ctx):
        """Initialize the bolt."""
        self.conf = conf
        self.ctx = ctx
        
        # Configuration
        self.max_cache_size = int(conf.get('max_dedup_cache_size', 10000))
        
        # Deduplication caches (OrderedDict for LRU behavior)
        self.seen_tweet_ids = OrderedDict()
        self.seen_text_hashes = OrderedDict()
        
        # Statistics
        self.processed = 0
        self.duplicates_dropped = 0
        self.retweets_dropped = 0
        self.unique_emitted = 0
        
        logger.info(f"DedupBolt initialized with cache size {self.max_cache_size}")
    
    def process(self, tup):
        """
        Process incoming tuple from ParseBolt.
        
        Args:
            tup: Tuple containing parsed tweet data
        """
        try:
            # Extract tweet data
            tweet_data = tup.values[0]
            
            self.processed += 1
            
            # Check if retweet
            if tweet_data.get('retweet_status', False):
                self.retweets_dropped += 1
                self.ack(tup)
                
                if self.retweets_dropped % 100 == 0:
                    logger.info(f"Dropped {self.retweets_dropped} retweets")
                
                return
            
            # Check for duplicate tweet ID
            tweet_id = tweet_data.get('tweet_id')
            if self._is_duplicate_id(tweet_id):
                self.duplicates_dropped += 1
                self.ack(tup)
                
                if self.duplicates_dropped % 100 == 0:
                    logger.info(f"Dropped {self.duplicates_dropped} duplicate IDs")
                
                return
            
            # Check for duplicate text (near-duplicates)
            text = tweet_data.get('text', '')
            text_hash = self._compute_text_hash(text)
            
            if self._is_duplicate_hash(text_hash):
                self.duplicates_dropped += 1
                self.ack(tup)
                return
            
            # Not a duplicate - add to caches and emit
            self._add_to_cache(tweet_id, text_hash)
            
            self.emit([tweet_data], anchors=[tup])
            self.ack(tup)
            
            self.unique_emitted += 1
            
            # Log statistics every 1000 processed tweets
            if self.processed % 1000 == 0:
                self._log_statistics()
        
        except Exception as e:
            logger.error(f"Error processing tuple: {e}", exc_info=True)
            self.fail(tup)
    
    def _is_duplicate_id(self, tweet_id: str) -> bool:
        """
        Check if tweet ID has been seen before.
        
        Args:
            tweet_id: Tweet ID to check
            
        Returns:
            True if duplicate, False otherwise
        """
        if tweet_id in self.seen_tweet_ids:
            # Move to end (most recently used)
            self.seen_tweet_ids.move_to_end(tweet_id)
            return True
        return False
    
    def _is_duplicate_hash(self, text_hash: str) -> bool:
        """
        Check if text hash has been seen before.
        
        Args:
            text_hash: MD5 hash of tweet text
            
        Returns:
            True if duplicate, False otherwise
        """
        if text_hash in self.seen_text_hashes:
            # Move to end (most recently used)
            self.seen_text_hashes.move_to_end(text_hash)
            return True
        return False
    
    def _compute_text_hash(self, text: str) -> str:
        """
        Compute MD5 hash of tweet text.
        
        Args:
            text: Tweet text
            
        Returns:
            MD5 hash as hex string
        """
        # Normalize text before hashing
        normalized = text.lower().strip()
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def _add_to_cache(self, tweet_id: str, text_hash: str):
        """
        Add tweet ID and text hash to caches with LRU eviction.
        
        Args:
            tweet_id: Tweet ID
            text_hash: Text hash
        """
        # Add to tweet ID cache
        self.seen_tweet_ids[tweet_id] = True
        
        # Evict oldest if cache is full
        if len(self.seen_tweet_ids) > self.max_cache_size:
            self.seen_tweet_ids.popitem(last=False)
        
        # Add to text hash cache
        self.seen_text_hashes[text_hash] = True
        
        # Evict oldest if cache is full
        if len(self.seen_text_hashes) > self.max_cache_size:
            self.seen_text_hashes.popitem(last=False)
    
    def _log_statistics(self):
        """Log deduplication statistics."""
        total_dropped = self.duplicates_dropped + self.retweets_dropped
        drop_rate = (total_dropped / self.processed * 100) if self.processed > 0 else 0
        
        logger.info(
            f"DedupBolt stats: "
            f"processed={self.processed}, "
            f"unique={self.unique_emitted}, "
            f"duplicates={self.duplicates_dropped}, "
            f"retweets={self.retweets_dropped}, "
            f"drop_rate={drop_rate:.1f}%, "
            f"cache_size={len(self.seen_tweet_ids)}"
        )


if __name__ == "__main__":
    # For local testing
    
    bolt = DedupBolt()
    bolt.initialize({}, {})
    
    # Test duplicate detection
    test_tweets = [
        {'tweet_id': '123', 'text': 'Original tweet', 'retweet_status': False},
        {'tweet_id': '123', 'text': 'Original tweet', 'retweet_status': False},  # Duplicate ID
        {'tweet_id': '124', 'text': 'Original tweet', 'retweet_status': False},  # Duplicate text
        {'tweet_id': '125', 'text': 'RT @user: Retweet', 'retweet_status': True},  # Retweet
        {'tweet_id': '126', 'text': 'Unique tweet', 'retweet_status': False},  # Unique
    ]
    
    print("Testing DedupBolt:")
    for i, tweet in enumerate(test_tweets):
        print(f"\nTweet {i+1}: ID={tweet['tweet_id']}, RT={tweet['retweet_status']}")
        
        # Simulate processing
        is_retweet = tweet.get('retweet_status', False)
        is_dup_id = bolt._is_duplicate_id(tweet['tweet_id'])
        text_hash = bolt._compute_text_hash(tweet['text'])
        is_dup_text = bolt._is_duplicate_hash(text_hash)
        
        if is_retweet:
            print("  → Dropped (retweet)")
        elif is_dup_id:
            print("  → Dropped (duplicate ID)")
        elif is_dup_text:
            print("  → Dropped (duplicate text)")
        else:
            bolt._add_to_cache(tweet['tweet_id'], text_hash)
            print("  → Emitted (unique)")
    
    print(f"\nCache size: {len(bolt.seen_tweet_ids)} tweet IDs, {len(bolt.seen_text_hashes)} text hashes")
