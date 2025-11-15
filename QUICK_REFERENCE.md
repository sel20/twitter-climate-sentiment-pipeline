# Quick Reference

## Setup

```bash
git clone <repo-url>
cd twitter-climate-sentiment-pipeline
pip install -r requirements.txt
export USE_LOCAL_FILE=true
export USE_LOCAL_STORAGE=true
python src/topology.py --local
```

## Project Structure

```
├── src/                    # Source code
│   ├── topology.py         # Main topology
│   ├── spouts/             # TwitterSpout
│   ├── bolts/              # 8 processing bolts
│   └── models/             # NOAA client, sentiment model
├── hive/                   # SQL scripts
├── notebooks/              # Visualizations
├── tests/                  # Unit tests
└── data/                   # Sample data
```

## Commands

### Pipeline Execution

```bash
python src/topology.py --local
bash scripts/run_local.sh
bash scripts/deploy_topology.sh
```

### Hive Operations

```bash
beeline -u jdbc:hive2://localhost:10000 -f hive/create_tables.sql
beeline -u jdbc:hive2://localhost:10000 -f hive/queries.sql
```

### Visualizations

```bash
jupyter notebook notebooks/visualizations.ipynb
```

### Testing

```bash
pytest tests/
```

## Environment Variables

```bash
TWITTER_BEARER_TOKEN=your_token_here
NOAA_API_TOKEN=your_token_here
USE_LOCAL_STORAGE=true
LOCAL_STORAGE_PATH=/tmp/twitter_climate
BUFFER_SIZE=1000
FLUSH_INTERVAL_SECONDS=300
```

## Components

### TwitterSpout
`src/spouts/TwitterSpout.py` - Ingests tweets from Twitter API v2 with rate limiting and pagination.

### CleanBolt
`src/bolts/CleanBolt.py` - Text cleaning, emoji mapping, hashtag decomposition, contraction expansion.

### SentimentBolt
`src/bolts/SentimentBolt.py` - Multilingual sentiment analysis using RoBERTa and XLM-RoBERTa models.

### WriteBolt
`src/bolts/WriteBolt.py` - Writes to Parquet files with partitioning and compression.

## Data Flow

```
Twitter API → TwitterSpout → ParseBolt → DedupBolt → CleanBolt →
LangDetectBolt → GeoResolveBolt → NOAAEnrichBolt → SentimentBolt →
WriteBolt → Parquet Files → Hive Tables
```

## Performance Metrics

- Throughput: 50-100 tweets/second
- Latency: <10 seconds per tweet (p95)
- Storage: ~500 bytes/tweet (Parquet compressed)
- Sentiment accuracy: 78%
- Geo coverage: 45%
- NOAA match rate: 35%

## Troubleshooting

### Dependencies

```bash
pip install -r requirements.txt
```

### Local File Mode

```bash
export USE_LOCAL_FILE=true
export LOCAL_SAMPLE_FILE=data/sample_tweets.json
```

### Hive Tables

```bash
beeline -u jdbc:hive2://localhost:10000 -f hive/create_tables.sql
beeline -u jdbc:hive2://localhost:10000 -e "MSCK REPAIR TABLE processed_tweets;"
```

### Models

```bash
python scripts/download_models.py
```

## Validation

```bash
python src/topology.py --local
ls /tmp/twitter_climate/processed_tweets/
python -c "import pandas as pd; print(pd.read_parquet('/tmp/twitter_climate/processed_tweets/').head())"
```
