# Twitter Climate Sentiment Pipeline

CSC1142 Assignment - Topic 2: Twitter (X) Sentiment on Climate Events

---

## Project Overview

A streaming and batch data pipeline that ingests climate-related tweets, enriches them with NOAA weather data, computes multilingual sentiment scores, and produces analytical visualizations. Built using Apache Storm for real-time processing and Apache Hive for batch analytics.

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

## Setup Instructions

### 1. Clone Repository

```bash
git clone <repository-url>
cd twitter-climate-sentiment-pipeline
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure API Credentials

Copy the environment template and add your credentials:

```bash
cp .env.example .env
```

Edit `.env` and add:
- `TWITTER_BEARER_TOKEN`: Your Twitter API v2 bearer token
- `NOAA_API_TOKEN`: Your NOAA API token

**Important**: Never commit `.env` to version control!

### 4. Start Local Infrastructure (Development)

```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Storm Nimbus (port 6627)
- Storm Supervisor
- Storm UI (port 8080)
- Redis (port 6379)

### 5. Download Language Models

```bash
python scripts/download_models.py
```

This downloads:
- fastText language identification model
- RoBERTa sentiment models from Hugging Face

## Running the Pipeline

### Local Mode (Development)

```bash
bash scripts/run_local.sh
```

### Production Mode (Storm Cluster)

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
├── report/
│   └── final_report.pdf         # Assignment report
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

## Hive Queries

### Create Tables

```bash
beeline -u jdbc:hive2://localhost:10000 -f hive/create_tables.sql
```

### Run Analytical Queries

```bash
beeline -u jdbc:hive2://localhost:10000 -f hive/queries.sql
```

## Visualizations

Generate visualizations:

```bash
jupyter notebook notebooks/visualizations.ipynb
```

Outputs:
- `visualizations/negative_sentiment_heatmap.html` - Geographic heatmap
- `visualizations/sentiment_by_event.png` - Bar chart by event type
- `visualizations/top_negative_locations.html` - Top locations table
- `visualizations/sentiment_time_series.html` - Daily trend

## Testing

Run unit tests:

```bash
pytest tests/
```

Run integration test:

```bash
python tests/test_integration.py
```

## Monitoring

- **Storm UI**: http://localhost:8080
- **Metrics**: `python scripts/data_quality.py`

## Data Quality Metrics

Expected metrics:
- NOAA match rate: >30%
- High/medium geo confidence: >50%
- Sentiment model accuracy: >75%
- Processing latency (p95): <30 seconds

## Troubleshooting

### Twitter API Rate Limits

If you hit rate limits, the spout will automatically back off. Alternatively, use the sample dataset:

```bash
# Edit topology.py to use local file mode
USE_LOCAL_FILE = True
```

### HDFS Connection Issues

For local development, use local filesystem:

```bash
# In WriteBolt.py, set:
STORAGE_PATH = "file:///tmp/twitter_climate/"
```

### Model Download Failures

Models are cached in `~/.cache/huggingface/`. If downloads fail, manually download from:
- https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest
- https://huggingface.co/cardiffnlp/twitter-xlm-roberta-base-sentiment

## Team Contributions

- **Member 1**: Infrastructure setup, Twitter/NOAA integration, data cleaning, documentation
- **Member 2**: Sentiment analysis, geolocation, Hive queries, visualizations, testing

## 🚀 Quick Start (3 Steps)

### Step 1: Test Your Setup
```bash
# Run setup test (checks everything is working)
python test_setup.py
```

### Step 2: Run with Sample Data (No API Keys Required)
```bash
# Install dependencies
pip install pandas pyarrow emoji contractions

# Run pipeline
python src/topology.py --local
```

### Step 3: Check Output
```bash
# View processed tweets
python -c "import pandas as pd; print(pd.read_parquet('/tmp/twitter_climate/processed_tweets/').head())"
```

**That's it!** Your pipeline is running. See `GETTING_STARTED.md` for detailed instructions.


---

### Assignment Deliverables Checklist
- [ ] Run `python test_setup.py` 
- [ ] Run `python src/topology.py --local` 
- [ ] Push code to GitHub 
- [ ] Fill out `report/REPORT_TEMPLATE.md`
- [ ] Record video using `docs/VIDEO_SCRIPT.md`
- [ ] Submit: GitHub URL + Video URL + Report PDF

## License

MIT License

## References

- Twitter API v2: https://developer.twitter.com/en/docs/twitter-api
- NOAA API: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
- Apache Storm: https://storm.apache.org/
- Apache Hive: https://hive.apache.org/
- Hugging Face Transformers: https://huggingface.co/docs/transformers/



