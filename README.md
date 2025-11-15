# Twitter Climate Sentiment Pipeline

Real-time streaming pipeline for sentiment analysis of climate-related tweets using Apache Storm and Apache Hive.

## Overview

Streaming and batch data pipeline that ingests climate-related tweets, enriches them with NOAA weather data, computes multilingual sentiment scores, and produces analytical visualizations. Built using Apache Storm for real-time processing and Apache Hive for batch analytics.

## Architecture

- **Ingestion Layer**: Twitter API v2 via Storm TwitterSpout
- **Processing Layer**: Storm topology with 8 bolts (Parse, Dedup, Clean, LangDetect, GeoResolve, NOAAEnrich, Sentiment, Write)
- **Storage Layer**: HDFS/S3 with Parquet format (partitioned by date)
- **Analytics Layer**: Apache Hive external tables
- **Visualization Layer**: Jupyter notebooks with Folium and Plotly

## Features

- Real-time tweet ingestion with rate limit handling
- Non-trivial text cleaning (emoji mapping, hashtag decomposition, contraction expansion)
- Multi-tier geolocation resolution (coordinates → place centroid → geocoding)
- NOAA weather data enrichment with caching
- Multilingual sentiment analysis (RoBERTa for English, XLM-RoBERTa for others)
- Deduplication and data quality checks
- Geographic heatmaps and analytical visualizations

## Prerequisites

- Python 3.8+
- Apache Storm 2.4.0+
- Apache Hive 3.1.0+
- HDFS or S3-compatible storage
- Redis (for caching)
- Docker and Docker Compose (for local development)

## Installation

```bash
git clone <repository-url>
cd twitter-climate-sentiment-pipeline
pip install -r requirements.txt
cp .env.example .env
```

Configure `.env` with API credentials:
- `TWITTER_BEARER_TOKEN`: Twitter API v2 bearer token
- `NOAA_API_TOKEN`: NOAA API token

Download models:
```bash
python scripts/download_models.py
```

Start infrastructure:
```bash
docker-compose up -d
```

## Usage

Local mode:
```bash
bash scripts/run_local.sh
```

Production mode:
```bash
bash scripts/deploy_topology.sh
```

## Project Structure

```
twitter-climate-sentiment-pipeline/
├── src/
│   ├── topology.py              # Storm topology definition
│   ├── spouts/
│   │   └── TwitterSpout.py      # Twitter API ingestion
│   ├── bolts/
│   │   ├── ParseBolt.py         # JSON parsing
│   │   ├── DedupBolt.py         # Deduplication
│   │   ├── CleanBolt.py         # Text cleaning
│   │   ├── LangDetectBolt.py    # Language detection
│   │   ├── GeoResolveBolt.py    # Geolocation resolution
│   │   ├── NOAAEnrichBolt.py    # Weather enrichment
│   │   ├── SentimentBolt.py     # Sentiment analysis
│   │   └── WriteBolt.py         # Parquet writing
│   └── models/
│       ├── sentiment_model.py   # Sentiment model wrapper
│       └── noaa_client.py       # NOAA API client
├── hive/
│   ├── create_tables.sql        # Hive DDL
│   └── queries.sql              # Analytical queries
├── notebooks/
│   └── visualizations.ipynb     # Visualization notebook
├── scripts/
│   ├── deploy_topology.sh       # Production deployment
│   ├── run_local.sh             # Local development
│   ├── download_models.py       # Model download script
│   └── data_quality.py          # Quality metrics
├── data/
│   └── sample_tweets.json       # Development dataset
├── tests/
│   └── test_bolts.py            # Unit tests
├── docs/
│   ├── architecture.png         # Architecture diagram
│   └── data_quality_report.md   # Quality metrics
├── visualizations/              # Generated visualizations
├── docker-compose.yml           # Local infrastructure
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment template
├── .gitignore
└── README.md
```

## Data Flow

1. **TwitterSpout** polls Twitter API every 30s with climate keywords
2. **ParseBolt** extracts JSON fields into structured tuples
3. **DedupBolt** removes retweets and duplicates
4. **CleanBolt** normalizes text, maps emojis, decomposes hashtags
5. **LangDetectBolt** identifies tweet language
6. **GeoResolveBolt** extracts/infers geolocation with confidence
7. **NOAAEnrichBolt** queries NOAA API for weather data
8. **SentimentBolt** computes multilingual sentiment scores
9. **WriteBolt** writes Parquet files to HDFS (partitioned by date)
10. **Hive** provides SQL interface for analytics
11. **Jupyter** generates visualizations from Hive queries

## Hive Analytics

```bash
beeline -u jdbc:hive2://localhost:10000 -f hive/create_tables.sql
beeline -u jdbc:hive2://localhost:10000 -f hive/queries.sql
```

## Visualizations

```bash
jupyter notebook notebooks/visualizations.ipynb
```

Output files:
- `visualizations/negative_sentiment_heatmap.html`
- `visualizations/sentiment_by_event.png`
- `visualizations/top_negative_locations.html`
- `visualizations/sentiment_time_series.html`

## Testing

```bash
pytest tests/
python tests/test_integration.py
```

## Monitoring

Storm UI: http://localhost:8080

Data quality metrics:
```bash
python scripts/data_quality.py
```

Performance benchmarks:
- NOAA match rate: >30%
- Geo confidence (high/medium): >50%
- Sentiment accuracy: >75%
- Processing latency (p95): <30s

## Configuration

Local file mode (no API required):
```bash
export USE_LOCAL_FILE=true
export LOCAL_SAMPLE_FILE=data/sample_tweets.json
```

Local storage (no HDFS required):
```bash
export USE_LOCAL_STORAGE=true
export LOCAL_STORAGE_PATH=/tmp/twitter_climate
```

## Documentation

- [GETTING_STARTED.md](GETTING_STARTED.md) - Setup instructions
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Command reference
- [docs/CODE_WALKTHROUGH.md](docs/CODE_WALKTHROUGH.md) - Code documentation

## References

- [Twitter API v2](https://developer.twitter.com/en/docs/twitter-api)
- [NOAA API](https://www.ncdc.noaa.gov/cdo-web/webservices/v2)
- [Apache Storm](https://storm.apache.org/)
- [Apache Hive](https://hive.apache.org/)
- [Hugging Face Transformers](https://huggingface.co/docs/transformers/)

