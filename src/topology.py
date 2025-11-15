"""
Storm Topology Definition - Twitter Climate Sentiment Pipeline.

This module defines the complete Storm topology that processes tweets
from ingestion through sentiment analysis and storage.
"""

import os
import sys
import logging
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

# Import spouts and bolts
from spouts.TwitterSpout import TwitterSpout
from bolts.ParseBolt import ParseBolt
from bolts.DedupBolt import DedupBolt
from bolts.CleanBolt import CleanBolt
from bolts.LangDetectBolt import LangDetectBolt
from bolts.GeoResolveBolt import GeoResolveBolt
from bolts.NOAAEnrichBolt import NOAAEnrichBolt
from bolts.SentimentBolt import SentimentBolt
from bolts.WriteBolt import WriteBolt

# Storm imports
try:
    from streamparse import Grouping, Topology
    STREAMPARSE_AVAILABLE = True
except ImportError:
    STREAMPARSE_AVAILABLE = False
    logging.warning("streamparse not available, using local mode")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TwitterClimateTopology(Topology):
    """
    Twitter Climate Sentiment Analysis Topology.
    
    Data Flow:
    TwitterSpout → ParseBolt → DedupBolt → CleanBolt → LangDetectBolt →
    GeoResolveBolt → NOAAEnrichBolt → SentimentBolt → WriteBolt
    """
    
    # Spout
    twitter_spout = TwitterSpout.spec(
        name='twitter-spout',
        par=1  # Single instance for consistent ordering
    )
    
    # Bolts
    parse_bolt = ParseBolt.spec(
        name='parse-bolt',
        inputs=[twitter_spout],
        par=1
    )
    
    dedup_bolt = DedupBolt.spec(
        name='dedup-bolt',
        inputs=[parse_bolt],
        par=1  # Single instance for centralized dedup cache
    )
    
    clean_bolt = CleanBolt.spec(
        name='clean-bolt',
        inputs=[dedup_bolt],
        par=3  # CPU-intensive text processing
    )
    
    lang_detect_bolt = LangDetectBolt.spec(
        name='lang-detect-bolt',
        inputs=[clean_bolt],
        par=2
    )
    
    geo_resolve_bolt = GeoResolveBolt.spec(
        name='geo-resolve-bolt',
        inputs=[lang_detect_bolt],
        par=2  # I/O bound (geocoding)
    )
    
    noaa_enrich_bolt = NOAAEnrichBolt.spec(
        name='noaa-enrich-bolt',
        inputs=[geo_resolve_bolt],
        par=2  # I/O bound (NOAA API)
    )
    
    sentiment_bolt = SentimentBolt.spec(
        name='sentiment-bolt',
        inputs=[noaa_enrich_bolt],
        par=4  # CPU-intensive (ML models)
    )
    
    write_bolt = WriteBolt.spec(
        name='write-bolt',
        inputs=[sentiment_bolt],
        par=2
    )


def run_local_mode():
    """
    Run topology in local mode for development/testing.
    
    This simulates the Storm topology without requiring a Storm cluster.
    """
    logger.info("=" * 60)
    logger.info("Twitter Climate Sentiment Pipeline - LOCAL MODE")
    logger.info("=" * 60)
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Configuration summary
    logger.info("\nConfiguration:")
    logger.info(f"  Twitter API: {'Configured' if os.getenv('TWITTER_BEARER_TOKEN') else 'Not configured'}")
    logger.info(f"  NOAA API: {'Configured' if os.getenv('NOAA_API_TOKEN') else 'Not configured'}")
    logger.info(f"  Use local file: {os.getenv('USE_LOCAL_FILE', 'false')}")
    logger.info(f"  Storage: {os.getenv('LOCAL_STORAGE_PATH', '/tmp/twitter_climate')}")
    
    # Initialize components
    logger.info("\nInitializing components...")
    
    spout = TwitterSpout()
    spout.initialize({}, {})
    
    parse_bolt = ParseBolt()
    parse_bolt.initialize({}, {})
    
    dedup_bolt = DedupBolt()
    dedup_bolt.initialize({}, {})
    
    clean_bolt = CleanBolt()
    clean_bolt.initialize({}, {})
    
    lang_detect_bolt = LangDetectBolt()
    lang_detect_bolt.initialize({}, {})
    
    geo_resolve_bolt = GeoResolveBolt()
    geo_resolve_bolt.initialize({}, {})
    
    noaa_enrich_bolt = NOAAEnrichBolt()
    noaa_enrich_bolt.initialize({}, {})
    
    sentiment_bolt = SentimentBolt()
    sentiment_bolt.initialize({}, {})
    
    write_bolt = WriteBolt()
    write_bolt.initialize({}, {})
    
    logger.info("✓ All components initialized")
    
    # Process tweets
    logger.info("\nStarting tweet processing...")
    logger.info("Press Ctrl+C to stop\n")
    
    processed_count = 0
    max_tweets = int(os.getenv('MAX_TWEETS', '100'))
    
    try:
        while processed_count < max_tweets:
            # Simulate tuple flow through topology
            # In real Storm, this happens automatically
            
            # 1. Get tweet from spout
            spout.next_tuple()
            if not spout.tweet_buffer or spout.buffer_index >= len(spout.tweet_buffer):
                logger.info("No more tweets available")
                break
            
            tweet = spout.tweet_buffer[spout.buffer_index - 1]
            tuple_data = spout._create_tuple(tweet)
            
            if not tuple_data:
                continue
            
            # 2. Parse
            parsed = parse_bolt._parse_tweet(tuple_data)
            if not parsed:
                continue
            
            # 3. Dedup
            if parsed.get('retweet_status'):
                continue
            if dedup_bolt._is_duplicate_id(parsed['tweet_id']):
                continue
            text_hash = dedup_bolt._compute_text_hash(parsed['text'])
            if dedup_bolt._is_duplicate_hash(text_hash):
                continue
            dedup_bolt._add_to_cache(parsed['tweet_id'], text_hash)
            
            # 4. Clean
            cleaned = clean_bolt._clean_tweet(parsed)
            if not cleaned:
                continue
            
            # 5. Language detection
            lang_data = lang_detect_bolt._detect_language(cleaned)
            if lang_data:
                cleaned.update(lang_data)
            
            # 6. Geolocation
            geo_data = geo_resolve_bolt._resolve_geolocation(cleaned)
            if geo_data:
                cleaned.update(geo_data)
            
            # 7. NOAA enrichment
            noaa_data = noaa_enrich_bolt._enrich_with_noaa(cleaned)
            if noaa_data:
                cleaned = noaa_data
            
            # 8. Sentiment
            sentiment_data = sentiment_bolt._compute_sentiment(cleaned)
            if sentiment_data:
                cleaned.update(sentiment_data)
            
            # 9. Write (add to buffer)
            write_bolt.buffer.append(cleaned)
            
            processed_count += 1
            
            if processed_count % 10 == 0:
                logger.info(f"Processed {processed_count} tweets...")
        
        # Final flush
        if write_bolt.buffer:
            write_bolt._flush_buffer()
        
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Processing complete!")
        logger.info(f"{'=' * 60}")
        logger.info(f"Total processed: {processed_count}")
        logger.info(f"Records written: {write_bolt.records_written}")
        logger.info(f"Output location: {write_bolt.storage_root}")
        
    except KeyboardInterrupt:
        logger.info("\nStopping...")
        if write_bolt.buffer:
            write_bolt._flush_buffer()
    except Exception as e:
        logger.error(f"Error in local mode: {e}", exc_info=True)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Twitter Climate Sentiment Pipeline')
    parser.add_argument('--local', action='store_true', help='Run in local mode')
    args = parser.parse_args()
    
    if args.local or not STREAMPARSE_AVAILABLE:
        run_local_mode()
    else:
        # Submit to Storm cluster
        logger.info("Submitting topology to Storm cluster...")
        # This would be handled by streamparse CLI: sparse submit
        logger.info("Use: sparse submit")
