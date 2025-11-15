# Setup Instructions

## Prerequisites

- Python 3.8+
- pip package manager
- Git
- Docker (optional, for Storm cluster deployment)

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/YOUR_USERNAME/twitter-climate-sentiment-pipeline.git
cd twitter-climate-sentiment-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Download ML Models

```bash
python scripts/download_models.py
```

Downloads fastText language model, RoBERTa sentiment model, and XLM-RoBERTa multilingual model. Pipeline falls back to VADER if models unavailable.

### 4. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with API credentials:
- `TWITTER_BEARER_TOKEN`: Twitter API v2 bearer token
- `NOAA_API_TOKEN`: NOAA CDO web services token

Pipeline operates with sample data if API keys not configured.

## Running the Pipeline

### Local Mode with Sample Data

```bash
export USE_LOCAL_FILE=true
export USE_LOCAL_STORAGE=true
export LOCAL_STORAGE_PATH=/tmp/twitter_climate
python src/topology.py --local
```

### Docker Deployment

```bash
docker-compose up -d
sleep 30
bash scripts/run_local.sh
```

## Verification

```bash
ls /tmp/twitter_climate/processed_tweets/

python -c "
import pandas as pd
df = pd.read_parquet('/tmp/twitter_climate/processed_tweets/')
print(df.head())
print(f'Total records: {len(df)}')
print(f'Sentiment distribution:\n{df.sentiment_label.value_counts()}')
"
```

## Hive Integration

```bash
hive --service metastore &
hive --service hiveserver2 &
beeline -u jdbc:hive2://localhost:10000 -f hive/create_tables.sql
beeline -u jdbc:hive2://localhost:10000 -f hive/queries.sql
```

## Visualizations

```bash
jupyter notebook notebooks/visualizations.ipynb
```

Output saved to `visualizations/` directory.

## Troubleshooting

### ModuleNotFoundError

```bash
pip install -r requirements.txt
```

### Models Not Loading

```bash
python scripts/download_models.py
```

### Permission Denied

```bash
chmod +x scripts/*.sh
```

### Redis Connection Failed

```bash
docker-compose up -d redis
```

### HDFS Not Available

```bash
export USE_LOCAL_STORAGE=true
export LOCAL_STORAGE_PATH=/tmp/twitter_climate
```

### Twitter API Rate Limit

```bash
export USE_LOCAL_FILE=true
export LOCAL_SAMPLE_FILE=data/sample_tweets.json
```

## Testing

```bash
pytest tests/
```

## Performance Metrics

- Sample data (50 tweets): ~10 seconds
- 100 tweets: ~2 minutes
- 1000 tweets: ~15 minutes
- Output: Parquet files (~500 bytes/tweet compressed)
