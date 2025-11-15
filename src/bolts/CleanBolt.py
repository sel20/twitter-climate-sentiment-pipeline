"""
CleanBolt - Applies non-trivial text cleaning and feature extraction.

This bolt performs advanced text normalization including:
- Unicode normalization
- Emoji mapping to sentiment tokens
- Hashtag decomposition (camelCase splitting)
- URL removal
- Contraction expansion
- Feature extraction
"""

import re
import logging
import unicodedata
from typing import Dict, Any, List, Tuple

# Import text processing libraries
try:
    import emoji
    EMOJI_AVAILABLE = True
except ImportError:
    EMOJI_AVAILABLE = False
    logging.warning("emoji library not available, emoji processing disabled")

try:
    import contractions
    CONTRACTIONS_AVAILABLE = True
except ImportError:
    CONTRACTIONS_AVAILABLE = False
    logging.warning("contractions library not available, contraction expansion disabled")

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


class CleanBolt(Bolt):
    """
    Storm bolt that performs non-trivial text cleaning and feature extraction.
    
    Features:
    - Unicode NFKC normalization
    - Emoji replacement with sentiment tokens
    - Emoji sentiment scoring
    - Hashtag decomposition (camelCase → separate words)
    - URL and media link removal
    - Contraction expansion
    - Feature extraction (counts, scores)
    """
    
    # Emoji sentiment lexicon (simplified - can be expanded)
    EMOJI_SENTIMENT = {
        '😊': 0.8, '😃': 0.9, '😄': 0.9, '😁': 0.8, '🙂': 0.6, '😀': 0.9,
        '😍': 1.0, '🥰': 1.0, '😘': 0.9, '💕': 0.9, '❤️': 0.9, '💖': 0.9,
        '👍': 0.7, '👏': 0.7, '🙌': 0.8, '✨': 0.6, '🌟': 0.7, '⭐': 0.6,
        '😢': -0.8, '😭': -0.9, '😞': -0.7, '😔': -0.7, '☹️': -0.6, '🙁': -0.6,
        '😠': -0.9, '😡': -1.0, '🤬': -1.0, '😤': -0.8, '💔': -0.9,
        '😱': -0.8, '😨': -0.7, '😰': -0.7, '😓': -0.6, '😖': -0.7,
        '🌞': 0.7, '☀️': 0.6, '🌈': 0.8, '🌸': 0.6, '🌺': 0.6,
        '🌊': 0.0, '💧': 0.0, '🌧️': -0.3, '⛈️': -0.5, '🌪️': -0.7,
        '🔥': -0.4, '💨': -0.2, '❄️': 0.0, '🌡️': 0.0
    }
    
    # URL pattern
    URL_PATTERN = re.compile(r'https?://\S+|www\.\S+')
    
    # Mention pattern
    MENTION_PATTERN = re.compile(r'@\w+')
    
    # Hashtag pattern
    HASHTAG_PATTERN = re.compile(r'#\w+')
    
    def initialize(self, conf, ctx):
        """Initialize the bolt."""
        self.conf = conf
        self.ctx = ctx
        
        # Statistics
        self.processed = 0
        self.failed = 0
        
        logger.info("CleanBolt initialized")
        logger.info(f"  Emoji processing: {EMOJI_AVAILABLE}")
        logger.info(f"  Contraction expansion: {CONTRACTIONS_AVAILABLE}")
    
    def process(self, tup):
        """
        Process incoming tuple from DedupBolt.
        
        Args:
            tup: Tuple containing deduplicated tweet data
        """
        try:
            # Extract tweet data
            tweet_data = tup.values[0]
            
            # Clean and extract features
            cleaned_data = self._clean_tweet(tweet_data)
            
            if not cleaned_data:
                logger.error(f"Failed to clean tweet {tweet_data.get('tweet_id')}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Emit cleaned tuple
            self.emit([cleaned_data], anchors=[tup])
            self.ack(tup)
            
            self.processed += 1
            if self.processed % 1000 == 0:
                logger.info(f"CleanBolt: processed {self.processed}, failed {self.failed}")
        
        except Exception as e:
            logger.error(f"Error processing tuple: {e}", exc_info=True)
            self.fail(tup)
            self.failed += 1
    
    def _clean_tweet(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply all cleaning transformations and extract features.
        
        Args:
            tweet_data: Raw tweet data
            
        Returns:
            Cleaned tweet data with additional features
        """
        try:
            text = tweet_data.get('text', '')
            
            # Step 1: Unicode normalization (NFKC)
            text = unicodedata.normalize('NFKC', text)
            
            # Step 2: Extract and process emojis
            emoji_data = self._process_emojis(text)
            text = emoji_data['text']
            
            # Step 3: Process hashtags (decompose camelCase)
            hashtag_data = self._process_hashtags(text, tweet_data.get('hashtags', []))
            text = hashtag_data['text']
            
            # Step 4: Remove URLs but keep hashtags and mentions
            text = self._remove_urls(text)
            
            # Step 5: Expand contractions
            text = self._expand_contractions(text)
            
            # Step 6: Additional normalization
            text = self._normalize_whitespace(text)
            
            # Step 7: Extract features
            features = self._extract_features(text, tweet_data, emoji_data, hashtag_data)
            
            # Build cleaned data
            cleaned = tweet_data.copy()
            cleaned['text_clean'] = text
            cleaned['text_original'] = tweet_data.get('text', '')
            cleaned.update(features)
            cleaned['processing_stage'] = 'cleaned'
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error cleaning tweet: {e}", exc_info=True)
            return None
    
    def _process_emojis(self, text: str) -> Dict[str, Any]:
        """
        Process emojis: extract, score, and replace with tokens.
        
        Args:
            text: Input text
            
        Returns:
            Dict with processed text and emoji data
        """
        if not EMOJI_AVAILABLE:
            return {
                'text': text,
                'emoji_list': [],
                'emoji_count': 0,
                'emoji_sentiment_score': 0.0
            }
        
        # Extract emojis
        emoji_list = [c for c in text if c in emoji.EMOJI_DATA]
        
        # Compute sentiment score
        sentiment_score = sum(self.EMOJI_SENTIMENT.get(e, 0.0) for e in emoji_list)
        
        # Replace emojis with textual tokens
        text_with_tokens = emoji.demojize(text, delimiters=(" :", ": "))
        
        return {
            'text': text_with_tokens,
            'emoji_list': emoji_list,
            'emoji_count': len(emoji_list),
            'emoji_sentiment_score': round(sentiment_score, 3)
        }
    
    def _process_hashtags(self, text: str, hashtags: List[str]) -> Dict[str, Any]:
        """
        Process hashtags: decompose camelCase and replace in text.
        
        Args:
            text: Input text
            hashtags: List of hashtag strings (without #)
            
        Returns:
            Dict with processed text and hashtag data
        """
        decomposed_hashtags = []
        
        for hashtag in hashtags:
            # Decompose camelCase: #ClimateStrike → climate strike
            decomposed = self._decompose_camelcase(hashtag)
            decomposed_hashtags.append(decomposed)
            
            # Replace in text
            text = text.replace(f'#{hashtag}', decomposed)
        
        return {
            'text': text,
            'decomposed_hashtags': decomposed_hashtags
        }
    
    def _decompose_camelcase(self, text: str) -> str:
        """
        Decompose camelCase text into separate words.
        
        Args:
            text: camelCase text
            
        Returns:
            Space-separated words
        """
        # Insert space before uppercase letters
        decomposed = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        
        # Insert space before numbers
        decomposed = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', decomposed)
        
        return decomposed.lower()
    
    def _remove_urls(self, text: str) -> str:
        """
        Remove URLs from text.
        
        Args:
            text: Input text
            
        Returns:
            Text with URLs removed
        """
        return self.URL_PATTERN.sub('', text)
    
    def _expand_contractions(self, text: str) -> str:
        """
        Expand contractions (don't → do not).
        
        Args:
            text: Input text
            
        Returns:
            Text with expanded contractions
        """
        if not CONTRACTIONS_AVAILABLE:
            return text
        
        try:
            return contractions.fix(text)
        except Exception as e:
            logger.warning(f"Error expanding contractions: {e}")
            return text
    
    def _normalize_whitespace(self, text: str) -> str:
        """
        Normalize whitespace: collapse multiple spaces, trim.
        
        Args:
            text: Input text
            
        Returns:
            Normalized text
        """
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Trim leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def _extract_features(
        self,
        text: str,
        tweet_data: Dict[str, Any],
        emoji_data: Dict[str, Any],
        hashtag_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract text features.
        
        Args:
            text: Cleaned text
            tweet_data: Original tweet data
            emoji_data: Emoji processing results
            hashtag_data: Hashtag processing results
            
        Returns:
            Feature dict
        """
        # Count features
        hashtag_count = tweet_data.get('hashtag_count', 0)
        mention_count = tweet_data.get('mention_count', 0)
        url_count = tweet_data.get('url_count', 0)
        
        # Text length
        text_length = len(text)
        word_count = len(text.split())
        
        # Check for media
        has_media = url_count > 0
        
        # Emoji features
        emoji_count = emoji_data['emoji_count']
        emoji_sentiment_score = emoji_data['emoji_sentiment_score']
        
        return {
            'hashtag_count': hashtag_count,
            'mention_count': mention_count,
            'url_count': url_count,
            'emoji_count': emoji_count,
            'emoji_sentiment_score': emoji_sentiment_score,
            'text_length': text_length,
            'word_count': word_count,
            'has_media': has_media,
            'avg_word_length': round(text_length / word_count, 2) if word_count > 0 else 0
        }


if __name__ == "__main__":
    # For local testing
    
    bolt = CleanBolt()
    bolt.initialize({}, {})
    
    # Test cases
    test_tweets = [
        {
            'tweet_id': '1',
            'text': "Devastating floods 😢 #ClimateChange is real! https://example.com",
            'hashtags': ['ClimateChange'],
            'hashtag_count': 1,
            'mention_count': 0,
            'url_count': 1
        },
        {
            'tweet_id': '2',
            'text': "Don't worry, we'll survive this storm 💪 @weatherservice",
            'hashtags': [],
            'hashtag_count': 0,
            'mention_count': 1,
            'url_count': 0
        },
        {
            'tweet_id': '3',
            'text': "Beautiful day after #HurricaneIda 🌞🌈",
            'hashtags': ['HurricaneIda'],
            'hashtag_count': 1,
            'mention_count': 0,
            'url_count': 0
        }
    ]
    
    print("Testing CleanBolt:\n")
    for tweet in test_tweets:
        print(f"Original: {tweet['text']}")
        cleaned = bolt._clean_tweet(tweet)
        if cleaned:
            print(f"Cleaned:  {cleaned['text_clean']}")
            print(f"Features: emoji_count={cleaned['emoji_count']}, "
                  f"emoji_sentiment={cleaned['emoji_sentiment_score']}, "
                  f"word_count={cleaned['word_count']}")
        print()
