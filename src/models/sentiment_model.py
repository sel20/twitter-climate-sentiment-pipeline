"""
Sentiment Model Wrapper - Multilingual sentiment analysis.

This module provides a unified interface for sentiment analysis using:
- RoBERTa for English tweets
- XLM-RoBERTa for multilingual tweets
- VADER as fallback
"""

import os
import logging
from typing import Dict, Any, Optional, Tuple

# Transformers for sentiment models
try:
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
    import torch
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    logging.warning("transformers not available, sentiment analysis disabled")

# VADER for fallback
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    logging.warning("vaderSentiment not available")

logger = logging.getLogger(__name__)


class SentimentModel:
    """
    Multilingual sentiment analysis model wrapper.
    
    Features:
    - Language-based model routing (English vs multilingual)
    - RoBERTa for English (twitter-roberta-base-sentiment-latest)
    - XLM-RoBERTa for other languages (twitter-xlm-roberta-base-sentiment)
    - VADER fallback for lightweight analysis
    - Emoji sentiment adjustment
    - Confidence scoring
    """
    
    # Model names
    MODEL_EN = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    MODEL_MULTILINGUAL = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
    
    # Sentiment thresholds
    POSITIVE_THRESHOLD = 0.3
    NEGATIVE_THRESHOLD = -0.3
    
    def __init__(self):
        """Initialize sentiment models."""
        self.model_en = None
        self.model_multilingual = None
        self.vader = None
        
        # Load models
        if TRANSFORMERS_AVAILABLE:
            self._load_transformer_models()
        
        if VADER_AVAILABLE:
            self.vader = SentimentIntensityAnalyzer()
            logger.info("VADER sentiment analyzer loaded")
        
        logger.info("SentimentModel initialized")
    
    def _load_transformer_models(self):
        """Load transformer-based sentiment models."""
        try:
            # Load English model
            logger.info(f"Loading English sentiment model: {self.MODEL_EN}")
            self.model_en = pipeline(
                "sentiment-analysis",
                model=self.MODEL_EN,
                tokenizer=self.MODEL_EN,
                device=-1  # CPU (-1), use 0 for GPU
            )
            logger.info("English sentiment model loaded")
        except Exception as e:
            logger.warning(f"Failed to load English model: {e}")
            self.model_en = None
        
        try:
            # Load multilingual model
            logger.info(f"Loading multilingual sentiment model: {self.MODEL_MULTILINGUAL}")
            self.model_multilingual = pipeline(
                "sentiment-analysis",
                model=self.MODEL_MULTILINGUAL,
                tokenizer=self.MODEL_MULTILINGUAL,
                device=-1  # CPU
            )
            logger.info("Multilingual sentiment model loaded")
        except Exception as e:
            logger.warning(f"Failed to load multilingual model: {e}")
            self.model_multilingual = None
    
    def compute_sentiment(
        self,
        text: str,
        language: str = 'en',
        emoji_sentiment_score: float = 0.0
    ) -> Dict[str, Any]:
        """
        Compute sentiment score for text.
        
        Args:
            text: Cleaned text to analyze
            language: Language code (e.g., 'en', 'es', 'fr')
            emoji_sentiment_score: Pre-computed emoji sentiment score
            
        Returns:
            Dict with sentiment_score, sentiment_label, sentiment_method, sentiment_confidence
        """
        try:
            # Route to appropriate model
            if language == 'en' and self.model_en:
                result = self._compute_with_transformer(text, self.model_en, 'roberta_en')
            elif self.model_multilingual:
                result = self._compute_with_transformer(text, self.model_multilingual, 'xlm_roberta')
            elif self.vader:
                result = self._compute_with_vader(text)
            else:
                # No models available
                logger.warning("No sentiment models available")
                return {
                    'sentiment_score': 0.0,
                    'sentiment_label': 'neutral',
                    'sentiment_method': 'unavailable',
                    'sentiment_confidence': 0.0
                }
            
            # Adjust for emoji sentiment (±0.2 max)
            emoji_adjustment = max(-0.2, min(0.2, emoji_sentiment_score * 0.2))
            adjusted_score = result['sentiment_score'] + emoji_adjustment
            
            # Clamp to [-1, 1]
            adjusted_score = max(-1.0, min(1.0, adjusted_score))
            
            # Assign label based on adjusted score
            if adjusted_score > self.POSITIVE_THRESHOLD:
                label = 'positive'
            elif adjusted_score < self.NEGATIVE_THRESHOLD:
                label = 'negative'
            else:
                label = 'neutral'
            
            return {
                'sentiment_score': round(adjusted_score, 3),
                'sentiment_label': label,
                'sentiment_method': result['sentiment_method'],
                'sentiment_confidence': result['sentiment_confidence']
            }
        
        except Exception as e:
            logger.error(f"Error computing sentiment: {e}")
            return {
                'sentiment_score': 0.0,
                'sentiment_label': 'neutral',
                'sentiment_method': 'error',
                'sentiment_confidence': 0.0
            }
    
    def _compute_with_transformer(
        self,
        text: str,
        model: Any,
        method_name: str
    ) -> Dict[str, Any]:
        """
        Compute sentiment using transformer model.
        
        Args:
            text: Text to analyze
            model: Hugging Face pipeline
            method_name: Method identifier
            
        Returns:
            Dict with sentiment_score, sentiment_method, sentiment_confidence
        """
        try:
            # Truncate to model max length (512 tokens)
            text = text[:512]
            
            # Get prediction
            result = model(text)[0]
            
            label = result['label'].upper()
            confidence = result['score']
            
            # Convert label to score
            if 'POSITIVE' in label or label == 'POS':
                sentiment_score = confidence
            elif 'NEGATIVE' in label or label == 'NEG':
                sentiment_score = -confidence
            else:  # NEUTRAL
                sentiment_score = 0.0
            
            return {
                'sentiment_score': sentiment_score,
                'sentiment_method': method_name,
                'sentiment_confidence': round(confidence, 3)
            }
        
        except Exception as e:
            logger.error(f"Transformer sentiment error: {e}")
            # Fallback to VADER if available
            if self.vader:
                return self._compute_with_vader(text)
            raise
    
    def _compute_with_vader(self, text: str) -> Dict[str, Any]:
        """
        Compute sentiment using VADER (fallback).
        
        Args:
            text: Text to analyze
            
        Returns:
            Dict with sentiment_score, sentiment_method, sentiment_confidence
        """
        try:
            scores = self.vader.polarity_scores(text)
            compound = scores['compound']  # Range: -1 to 1
            
            # VADER confidence is based on absolute compound score
            confidence = abs(compound)
            
            return {
                'sentiment_score': compound,
                'sentiment_method': 'vader_fallback',
                'sentiment_confidence': round(confidence, 3)
            }
        
        except Exception as e:
            logger.error(f"VADER sentiment error: {e}")
            raise


if __name__ == "__main__":
    # For local testing
    
    model = SentimentModel()
    
    # Test cases
    test_texts = [
        ("This is a great day! I love the weather.", "en", 0.8),
        ("Terrible floods destroying homes and lives.", "en", -0.5),
        ("The weather is okay, nothing special.", "en", 0.0),
        ("Este es un día maravilloso con buen clima.", "es", 0.5),
        ("Inondations terribles dans la région.", "fr", -0.3),
    ]
    
    print("Testing SentimentModel:\n")
    
    for text, lang, emoji_score in test_texts:
        print(f"Text: {text}")
        print(f"Language: {lang}, Emoji score: {emoji_score}")
        
        result = model.compute_sentiment(text, lang, emoji_score)
        
        print(f"  → Score: {result['sentiment_score']}")
        print(f"  → Label: {result['sentiment_label']}")
        print(f"  → Method: {result['sentiment_method']}")
        print(f"  → Confidence: {result['sentiment_confidence']}")
        print()
