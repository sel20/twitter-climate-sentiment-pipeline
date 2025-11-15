#!/usr/bin/env python3
"""
Download required models for the Twitter Climate Sentiment Pipeline.
This script downloads:
- fastText language identification model
- RoBERTa sentiment models from Hugging Face
"""

import os
import urllib.request
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForSequenceClassification

def download_fasttext_model():
    """Download fastText language identification model."""
    print("Downloading fastText language identification model...")
    
    model_dir = Path("models/downloads")
    model_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = model_dir / "lid.176.bin"
    
    if model_path.exists():
        print(f"  ✓ Model already exists at {model_path}")
        return
    
    url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
    
    print(f"  Downloading from {url}...")
    urllib.request.urlretrieve(url, model_path)
    print(f"  ✓ Downloaded to {model_path}")

def download_sentiment_models():
    """Download sentiment analysis models from Hugging Face."""
    models = [
        ("cardiffnlp/twitter-roberta-base-sentiment-latest", "English sentiment model"),
        ("cardiffnlp/twitter-xlm-roberta-base-sentiment", "Multilingual sentiment model")
    ]
    
    for model_name, description in models:
        print(f"\nDownloading {description}...")
        print(f"  Model: {model_name}")
        
        try:
            # Download tokenizer and model (cached in ~/.cache/huggingface/)
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            model = AutoModelForSequenceClassification.from_pretrained(model_name)
            print(f"  ✓ Successfully downloaded {model_name}")
        except Exception as e:
            print(f"  ✗ Error downloading {model_name}: {e}")
            print(f"    You can manually download from: https://huggingface.co/{model_name}")

def main():
    print("=" * 60)
    print("Twitter Climate Sentiment Pipeline - Model Download")
    print("=" * 60)
    
    # Download fastText model
    download_fasttext_model()
    
    # Download sentiment models
    download_sentiment_models()
    
    print("\n" + "=" * 60)
    print("Model download complete!")
    print("=" * 60)
    print("\nModels are cached in:")
    print("  - fastText: models/downloads/")
    print("  - Transformers: ~/.cache/huggingface/")

if __name__ == "__main__":
    main()
