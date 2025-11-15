#!/bin/bash
# Deploy Storm topology to production cluster

set -e

echo "=========================================="
echo "Twitter Climate Sentiment Pipeline"
echo "Deploying to STORM CLUSTER"
echo "=========================================="

# Check if .env exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and configure your API credentials"
    exit 1
fi

# Load environment variables
export $(cat .env | grep -v '^#' | xargs)

TOPOLOGY_NAME="twitter-climate-sentiment"
NIMBUS_HOST=${STORM_NIMBUS_HOST:-localhost}
NIMBUS_PORT=${STORM_NIMBUS_PORT:-6627}

echo ""
echo "Configuration:"
echo "  Topology Name: ${TOPOLOGY_NAME}"
echo "  Nimbus: ${NIMBUS_HOST}:${NIMBUS_PORT}"
echo "  Storage: ${HDFS_NAMENODE}${STORAGE_ROOT}"
echo ""

# Check if Storm cluster is accessible
if ! nc -z ${NIMBUS_HOST} ${NIMBUS_PORT} 2>/dev/null; then
    echo "Error: Cannot connect to Storm Nimbus at ${NIMBUS_HOST}:${NIMBUS_PORT}"
    echo "Please ensure Storm cluster is running"
    exit 1
fi

# Package topology
echo "Packaging topology..."
cd src
tar -czf ../topology.tar.gz *.py spouts/ bolts/ models/
cd ..

echo "Submitting topology to Storm cluster..."
storm jar topology.tar.gz topology.py ${TOPOLOGY_NAME}

echo ""
echo "=========================================="
echo "Topology deployed successfully!"
echo "=========================================="
echo ""
echo "Monitor at: http://${NIMBUS_HOST}:8080"
echo ""
echo "To kill topology:"
echo "  storm kill ${TOPOLOGY_NAME}"
