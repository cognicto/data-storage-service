# Sensor Data Storage Microservice

A comprehensive microservice for consuming sensor data from Kafka, storing it hierarchically in local storage, uploading to Azure Blob Storage, and managing local file cleanup.

## Features

- 🚀 **Kafka Consumer**: Consumes sensor data from multiple topics with regex patterns
- 📁 **Hierarchical Storage**: Organizes data by asset_id/yyyy/mm/dd/hh/sensor.parquet
- ☁️ **Azure Integration**: Automatic upload to Azure Blob Storage with retry logic
- 🧹 **Smart Cleanup**: Configurable local file cleanup after successful uploads
- 🐳 **Docker Ready**: Complete containerization with docker-compose
- 📊 **Monitoring**: Health checks and metrics endpoints
- ⚙️ **Configurable**: Environment-based configuration for all settings

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kafka Cluster │───▶│  Storage Service  │───▶│  Azure Blob     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │ Local File System│
                       │ (Hierarchical)   │
                       └──────────────────┘
```

## Quick Start

1. **Clone and Setup**:
   ```bash
   git clone <repository-url>
   cd sensor-data-storage-service
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Run with Docker Compose**:
   ```bash
   docker-compose up -d
   ```

3. **Check Health**:
   ```bash
   curl http://localhost:8080/health
   ```

## Configuration

All configuration is done through environment variables. See `.env.example` for all available options.

### Key Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC_PATTERN`: Regex pattern for topic subscription
- `AZURE_STORAGE_ACCOUNT`: Azure storage account name
- `AZURE_STORAGE_KEY`: Azure storage access key
- `LOCAL_STORAGE_PATH`: Local storage directory
- `CLEANUP_ENABLED`: Enable/disable local file cleanup
- `CLEANUP_AGE_DAYS`: Age threshold for file cleanup

## Development

### Local Development

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Service**:
   ```bash
   python -m app.main
   ```

### Docker Development

```bash
# Build image
docker build -t sensor-storage-service .

# Run container
docker run --env-file .env sensor-storage-service
```

## API Endpoints

- `GET /health` - Service health check
- `GET /metrics` - Service metrics
- `POST /upload/trigger` - Manually trigger upload
- `POST /cleanup/trigger` - Manually trigger cleanup

## Monitoring

The service provides comprehensive logging and metrics:
- Kafka consumption metrics
- File processing statistics
- Azure upload success/failure rates
- Local storage usage

## Production Deployment

See [deployment/README.md](deployment/README.md) for production deployment guides including:
- Kubernetes manifests
- Azure Container Instances
- AWS ECS deployment
- Monitoring and alerting setup# data-storage-service
