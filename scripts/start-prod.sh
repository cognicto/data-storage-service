#!/bin/bash
# Production startup script

set -e

echo "Starting Sensor Data Storage Service in production mode..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "ERROR: .env file not found!"
    echo "Please create .env file with your production configuration."
    exit 1
fi

# Validate required environment variables
source .env

required_vars=(
    "KAFKA_BOOTSTRAP_SERVERS"
    "AZURE_STORAGE_ACCOUNT"
    "AZURE_STORAGE_KEY"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "ERROR: Required environment variable $var is not set"
        exit 1
    fi
done

# Create necessary directories
mkdir -p data/raw logs

# Pull latest images
echo "Pulling latest images..."
docker-compose pull

# Start production services (without dev profile)
echo "Starting production services..."
docker-compose up -d

echo "Production services started!"
echo ""
echo "Checking health..."
sleep 10

# Health check
if curl -f http://localhost:8080/health > /dev/null 2>&1; then
    echo "✓ Service is healthy"
else
    echo "✗ Service health check failed"
    echo "Check logs with: docker-compose logs sensor-storage-service"
    exit 1
fi

echo ""
echo "Service is running successfully!"
echo "Monitor with: docker-compose logs -f sensor-storage-service"