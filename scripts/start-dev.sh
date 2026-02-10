#!/bin/bash
# Development startup script

set -e

echo "Starting Sensor Data Storage Service in development mode..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env file from example..."
    cp .env.example .env
    echo "Please edit .env with your configuration and run again."
    exit 1
fi

# Create necessary directories
mkdir -p data/raw logs

# Start with docker-compose
echo "Starting services with docker-compose..."
docker-compose --profile dev up -d

echo "Services started!"
echo ""
echo "Available endpoints:"
echo "  - API: http://localhost:8080"
echo "  - Health: http://localhost:8080/health"
echo "  - Metrics: http://localhost:8080/metrics"
echo "  - Kafka: localhost:9092"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f sensor-storage-service"
echo ""
echo "To stop:"
echo "  docker-compose down"