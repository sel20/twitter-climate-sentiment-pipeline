"""
SentimentBolt - Computes multilingual sentiment scores.

This bolt applies sentiment analysis to cleaned tweet text,
routing to appropriate models based on detected language.
"""

import os
import logging
import sys
from typing import Dict, Any, Optional

# Add models to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from models.sentiment_model import SentimentModel

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


class SentimentBolt(Bolt):
    """
    Storm bolt that computes multilingual sentiment scores.
    
    Features:
    - Language-based model routing (English vs multilingual)
    - RoBERTa for English tweets
    - XLM-RoBERTa for non-English tweets
    - VADER fallback if models unavailable
    - Emoji sentiment adjustment (±0.2)
    - Confidence scoring
    - Categorical labeling (positive/neutral/negative)
    """
    
    def initialize(self, conf, ctx):
        """Initialize the bolt and load sentiment models."""
        self.conf = conf
        self.ctx = ctx
        
        # Load sentiment model (models loaded once at initialization)
        logger.info("Loading sentiment models...")
        self.sentiment_model = SentimentModel()
        logger.info("Sentiment models loaded")
        
        # Statistics
        self.processed = 0
        self.failed = 0
        self.positive_count = 0
        self.neutral_count = 0
        self.negative_count = 0
        
        logger.info("SentimentBolt initialized")
    
    def process(self, tup):
        """
        Process incoming tuple from NOAAEnrichBolt.
        
        Args:
            tup: Tuple containing NOAA-enriched tweet data
        """
        try:
            # Extract tweet data
            tweet_data = tup.values[0]
            
            # Compute sentiment
            sentiment_data = self._compute_sentiment(tweet_data)
            
            if not sentiment_data:
                logger.error(f"Failed to compute sentiment for tweet {tweet_data.get('tweet_id')}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Add sentiment data to tweet
            enriched_data = tweet_data.copy()
            enriched_data.update(sentiment_data)
            enriched_data['processing_stage'] = 'sentiment_computed'
            
            # Update label counts
            label = sentiment_data['sentiment_label']
            if label == 'positive':
                self.positive_count += 1
            elif label == 'negative':
                self.negative_count += 1
            else:
                self.neutral_count += 1
            
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
    
    def _compute_sentiment(self, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Compute sentiment for tweet.
        
        Args:
            tweet_data: Tweet data with text_clean and detected_lang
            
        Returns:
            Dict with sentiment fields or None
        """
        try:
            # Extract required fields
            text = tweet_data.get('text_clean', '')
            language = tweet_data.get('detected_lang', 'en')
            emoji_sentiment_score = tweet_data.get('emoji_sentiment_score', 0.0)
            
            # Validate text
            if not text or not text.strip():
                logger.warning(f"Empty text for tweet {tweet_data.get('tweet_id')}")
                return {
                    'sentiment_score': 0.0,
                    'sentiment_label': 'neutral',
                    'sentiment_method': 'empty_text',
                    'sentiment_confidence': 0.0
                }
            
            # Compute sentiment using model
            sentiment_result = self.sentiment_model.compute_sentiment(
                text=text,
                language=language,
                emoji_sentiment_score=emoji_sentiment_score
            )
            
            return sentiment_result
        
        except Exception as e:
            logger.error(f"Error computing sentiment: {e}")
            return None
    
    def _log_statistics(self):
        """Log sentiment analysis statistics."""
        total = self.processed
        if total == 0:
            return
        
        pos_pct = (self.positive_count / total * 100) if total > 0 else 0
        neu_pct = (self.neutral_count / total * 100) if total > 0 else 0
        neg_pct = (self.negative_count / total * 100) if total > 0 else 0
        
        logger.info(
            f"SentimentBolt stats: "
            f"processed={self.processed}, "
            f"failed={self.failed}, "
            f"positive={self.positive_count} ({pos_pct:.1f}%), "
            f"neutral={self.neutral_count} ({neu_pct:.1f}%), "
            f"negative={self.negative_count} ({neg_pct:.1f}%)"
        )


if __name__ == "__main__":
    # For local testing
    
    bolt = SentimentBolt()
    bolt.initialize({}, {})
    
    # Test tweets
    test_tweets = [
        {
            'tweet_id': '1',
            'text_clean': 'This is a great day with beautiful weather',
            'detected_lang': 'en',
            'emoji_sentiment_score': 0.8
        },
        {
            'tweet_id': '2',
            'text_clean': 'Terrible floods destroying homes',
            'detected_lang': 'en',
            'emoji_sentiment_score': -0.5
        },
        {
            'tweet_id': '3',
            'text_clean': 'The weather is okay today',
            'detected_lang': 'en',
            'emoji_sentiment_score': 0.0
        }
    ]
    
    print("Testing SentimentBolt:\n")
    
    for tweet in test_tweets:
        print(f"Text: {tweet['text_clean']}")
        result = bolt._compute_sentiment(tweet)
        if result:
            print(f"  Score: {result['sentiment_score']}")
            print(f"  Label: {result['sentiment_label']}")
            print(f"  Method: {result['sentiment_method']}")
            print(f"  Confidence: {result['sentiment_confidence']}")
        print()
