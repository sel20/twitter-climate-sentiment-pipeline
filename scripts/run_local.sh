#!/bin/bash
# Run Storm topology in local mode for development

set -e

echo "=========================================="
echo "Twitter Climate Sentiment Pipeline"
echo "Running in LOCAL MODE"
echo "=========================================="

# Check if .env exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and configure your API credentials"
    exit 1
fi

# Load environment variables
export $(cat .env | grep -v '^#' | xargs)

# Set local mode flag
export USE_LOCAL_STORAGE=true
export LOCAL_STORAGE_PATH=/tmp/twitter_climate

# Create local storage directories
mkdir -p /tmp/twitter_climate/processed_tweets
mkdir -p /tmp/twitter_climate/raw_tweets

echo ""
echo "Configuration:"
echo "  Storage: Local filesystem (/tmp/twitter_climate)"
echo "  Twitter API: ${USE_LOCAL_FILE:-false}"
echo "  Redis: ${REDIS_HOST}:${REDIS_PORT}"
echo ""

# Check if Redis is running
if ! nc -z ${REDIS_HOST} ${REDIS_PORT} 2>/dev/null; then
    echo "Warning: Redis is not running on ${REDIS_HOST}:${REDIS_PORT}"
    echo "Starting Redis with Docker Compose..."
    docker-compose up -d redis
    sleep 3
fi

echo "Starting Storm topology in local mode..."
echo ""

# Run topology in local mode
python src/topology.py --local

echo ""
echo "=========================================="
echo "Topology stopped"
echo "=========================================="
