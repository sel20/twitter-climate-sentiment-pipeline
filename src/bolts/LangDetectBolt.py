"""
LangDetectBolt - Detects tweet language for multilingual sentiment routing.

This bolt uses fastText language identification model to detect the language
of cleaned tweet text, with fallback to Twitter's lang field.
"""

import os
import logging
from typing import Dict, Any, Optional

# Language detection libraries
try:
    import fasttext
    FASTTEXT_AVAILABLE = True
except ImportError:
    FASTTEXT_AVAILABLE = False
    logging.warning("fasttext not available, will use Twitter lang field only")

try:
    from langdetect import detect, detect_langs
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False
    logging.warning("langdetect not available as fallback")

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


class LangDetectBolt(Bolt):
    """
    Storm bolt that detects tweet language using fastText.
    
    Features:
    - fastText language identification (176 languages)
    - Confidence scoring
    - Fallback to Twitter's lang field if confidence < 0.5
    - Fallback to langdetect library if fastText unavailable
    - Language code normalization (ISO 639-1)
    """
    
    # Confidence threshold for fastText predictions
    CONFIDENCE_THRESHOLD = 0.5
    
    # Default fastText model path
    DEFAULT_MODEL_PATH = "models/downloads/lid.176.bin"
    
    def initialize(self, conf, ctx):
        """Initialize the bolt and load language detection model."""
        self.conf = conf
        self.ctx = ctx
        
        # Load fastText model
        self.model = None
        self.model_loaded = False
        
        if FASTTEXT_AVAILABLE:
            self._load_fasttext_model()
        
        # Statistics
        self.processed = 0
        self.failed = 0
        self.fasttext_used = 0
        self.twitter_fallback = 0
        self.langdetect_fallback = 0
        
        logger.info("LangDetectBolt initialized")
        logger.info(f"  fastText available: {FASTTEXT_AVAILABLE}")
        logger.info(f"  fastText model loaded: {self.model_loaded}")
        logger.info(f"  langdetect available: {LANGDETECT_AVAILABLE}")
    
    def _load_fasttext_model(self):
        """Load fastText language identification model."""
        model_path = os.getenv('FASTTEXT_MODEL_PATH', self.DEFAULT_MODEL_PATH)
        
        try:
            # Suppress fastText warnings
            fasttext.FastText.eprint = lambda x: None
            
            if os.path.exists(model_path):
                self.model = fasttext.load_model(model_path)
                self.model_loaded = True
                logger.info(f"Loaded fastText model from {model_path}")
            else:
                logger.warning(f"fastText model not found at {model_path}")
                logger.warning("Run 'python scripts/download_models.py' to download")
        except Exception as e:
            logger.error(f"Failed to load fastText model: {e}")
            self.model = None
            self.model_loaded = False
    
    def process(self, tup):
        """
        Process incoming tuple from CleanBolt.
        
        Args:
            tup: Tuple containing cleaned tweet data
        """
        try:
            # Extract tweet data
            tweet_data = tup.values[0]
            
            # Detect language
            lang_data = self._detect_language(tweet_data)
            
            if not lang_data:
                logger.error(f"Failed to detect language for tweet {tweet_data.get('tweet_id')}")
                self.fail(tup)
                self.failed += 1
                return
            
            # Add language data to tweet
            enriched_data = tweet_data.copy()
            enriched_data.update(lang_data)
            enriched_data['processing_stage'] = 'lang_detected'
            
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
    
    def _detect_language(self, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect language of tweet text.
        
        Args:
            tweet_data: Tweet data with text_clean field
            
        Returns:
            Dict with detected_lang and lang_confidence
        """
        try:
            text = tweet_data.get('text_clean', '')
            twitter_lang = tweet_data.get('lang', 'unknown')
            
            # If text is too short, use Twitter's lang
            if len(text.strip()) < 10:
                return {
                    'detected_lang': twitter_lang,
                    'lang_confidence': 0.5,
                    'lang_method': 'twitter_short_text'
                }
            
            # Try fastText first
            if self.model_loaded and self.model:
                result = self._detect_with_fasttext(text)
                if result and result['lang_confidence'] >= self.CONFIDENCE_THRESHOLD:
                    self.fasttext_used += 1
                    return result
            
            # Try langdetect as fallback
            if LANGDETECT_AVAILABLE:
                result = self._detect_with_langdetect(text)
                if result:
                    self.langdetect_fallback += 1
                    return result
            
            # Final fallback to Twitter's lang field
            self.twitter_fallback += 1
            return {
                'detected_lang': twitter_lang,
                'lang_confidence': 0.3,
                'lang_method': 'twitter_fallback'
            }
        
        except Exception as e:
            logger.error(f"Error detecting language: {e}")
            return None
    
    def _detect_with_fasttext(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Detect language using fastText model.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dict with language and confidence or None
        """
        try:
            # Predict language (k=1 for top prediction)
            predictions = self.model.predict(text.replace('\n', ' '), k=1)
            
            # Extract language code and confidence
            lang_label = predictions[0][0]  # e.g., '__label__en'
            confidence = float(predictions[1][0])
            
            # Remove '__label__' prefix
            lang_code = lang_label.replace('__label__', '')
            
            return {
                'detected_lang': lang_code,
                'lang_confidence': round(confidence, 3),
                'lang_method': 'fasttext'
            }
        
        except Exception as e:
            logger.warning(f"fastText detection failed: {e}")
            return None
    
    def _detect_with_langdetect(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Detect language using langdetect library (fallback).
        
        Args:
            text: Text to analyze
            
        Returns:
            Dict with language and confidence or None
        """
        try:
            # Get language with confidence
            langs = detect_langs(text)
            
            if langs:
                top_lang = langs[0]
                return {
                    'detected_lang': top_lang.lang,
                    'lang_confidence': round(top_lang.prob, 3),
                    'lang_method': 'langdetect'
                }
            
            return None
        
        except Exception as e:
            logger.warning(f"langdetect detection failed: {e}")
            return None
    
    def _log_statistics(self):
        """Log language detection statistics."""
        total = self.processed
        if total == 0:
            return
        
        fasttext_pct = (self.fasttext_used / total * 100) if total > 0 else 0
        twitter_pct = (self.twitter_fallback / total * 100) if total > 0 else 0
        langdetect_pct = (self.langdetect_fallback / total * 100) if total > 0 else 0
        
        logger.info(
            f"LangDetectBolt stats: "
            f"processed={self.processed}, "
            f"failed={self.failed}, "
            f"fasttext={self.fasttext_used} ({fasttext_pct:.1f}%), "
            f"langdetect={self.langdetect_fallback} ({langdetect_pct:.1f}%), "
            f"twitter_fallback={self.twitter_fallback} ({twitter_pct:.1f}%)"
        )


if __name__ == "__main__":
    # For local testing
    
    bolt = LangDetectBolt()
    bolt.initialize({}, {})
    
    # Test cases
    test_tweets = [
        {
            'tweet_id': '1',
            'text_clean': 'This is a test tweet in English about climate change',
            'lang': 'en'
        },
        {
            'tweet_id': '2',
            'text_clean': 'Este es un tweet de prueba en español sobre el cambio climático',
            'lang': 'es'
        },
        {
            'tweet_id': '3',
            'text_clean': 'Ceci est un tweet de test en français sur le changement climatique',
            'lang': 'fr'
        },
        {
            'tweet_id': '4',
            'text_clean': 'Short',  # Too short
            'lang': 'en'
        }
    ]
    
    print("Testing LangDetectBolt:\n")
    for tweet in test_tweets:
        print(f"Text: {tweet['text_clean'][:50]}...")
        print(f"Twitter lang: {tweet['lang']}")
        
        result = bolt._detect_language(tweet)
        if result:
            print(f"Detected: {result['detected_lang']} "
                  f"(confidence: {result['lang_confidence']}, "
                  f"method: {result['lang_method']})")
        print()
