# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Essential Commands
- `make run` - Run service locally (starts main Python application)
- `make test` - Run pytest test suite with verbose output
- `make lint` - Run flake8 and black linting checks
- `make format` - Auto-format code with black and isort
- `make install` - Install Python dependencies from requirements.txt

### Development Environment
- `make dev` - Start development environment using start-dev.sh script
- `make compose-dev` - Start full development stack with Kafka using docker-compose
- `python -m app.main` - Direct Python execution of main service

### Testing
- `pytest tests/ -v` - Run tests with verbose output
- `pytest tests/ --cov=app --cov-report=html` - Run tests with coverage report
- `make test-kafka` - Run Kafka test producer script

### Docker Operations
- `make docker-build` - Build Docker image as sensor-storage-service:latest
- `make docker-run` - Run containerized service with .env file
- `make compose-up` - Start service with docker-compose
- `make compose-down` - Stop docker-compose stack

### Kubernetes
- `make k8s-deploy` - Deploy to Kubernetes cluster
- `make k8s-logs` - View Kubernetes logs
- `make k8s-delete` - Remove Kubernetes deployment

## Architecture Overview

### Core Components
This is a Python-based microservice that consumes sensor data from Kafka, stores it in hierarchical Parquet format, and manages lifecycle with Azure Blob Storage integration.

**Main Service Class**: `SensorDataStorageService` in `app/main.py` orchestrates all components:
- Kafka Consumer (`app/kafka/consumer.py`) - Consumes from topic patterns
- Storage Manager (`app/storage/manager.py`) - Hierarchical Parquet storage
- Azure Uploader (`app/azure/uploader.py`) - Parallel cloud uploads with retry logic
- Cleanup Service (`app/cleanup/service.py`) - Smart local file cleanup
- Aggregation Scheduler (`app/aggregation/scheduler.py`) - Pre-computed aggregations
- FastAPI REST API (`app/api/routes.py`) - Health, metrics, manual triggers

### Data Storage Hierarchy
Files are organized as: `asset_id/yyyy/mm/dd/hh/sensor_name.parquet`

**Raw data**: `/data/raw/asset_001/2024/01/01/00/sensor_temp.parquet`
**Aggregated data**: `/data/aggregated/asset_001/2024/01/01/sensor_temp_hour.parquet`

### Configuration System
All configuration is environment-based using dataclasses in `app/config.py`:
- `KafkaConfig` - Consumer settings, topic patterns, timeouts
- `AzureConfig` - Storage account, retry logic, parallel workers
- `StorageConfig` - Local paths, compression, file size limits
- `CleanupConfig` - Retention policies, cleanup intervals
- `ServiceConfig` - API host/port, debug settings

### Threading Model
The service uses a multi-threaded architecture:
- Main thread: Signal handling and coordination
- Kafka thread: Message consumption (daemon thread)
- Upload thread: Periodic Azure uploads (daemon thread) 
- API thread: FastAPI server (daemon thread)
- Cleanup scheduler: Background cleanup operations
- Aggregation scheduler: Background data aggregation

## Key Dependencies

### Core Framework
- **FastAPI**: REST API framework with automatic OpenAPI docs
- **uvicorn**: ASGI server for FastAPI
- **confluent-kafka**: High-performance Kafka consumer

### Data Processing
- **pandas**: Data manipulation and aggregation
- **pyarrow**: Parquet file format support
- **numpy**: Numerical computing

### Cloud Integration
- **azure-storage-blob**: Azure Blob Storage SDK
- **azure-core**: Azure SDK core functionality

### Development Tools
- **pytest**: Testing framework with asyncio support
- **black**: Code formatting
- **flake8**: Linting and style checking
- **mypy**: Static type checking
- **isort**: Import sorting

## Development Patterns

### Error Handling
All major operations use try-catch blocks with structured logging. Service continues operating despite individual component failures.

### Configuration Loading
Configuration is loaded once at startup from environment variables with validation. No runtime configuration changes are supported.

### Threading Safety
Components use thread-safe operations and daemon threads for clean shutdown. Signal handlers (SIGINT/SIGTERM) provide graceful shutdown.

### Resource Management
- In-memory buffers with size limits to prevent memory overflow
- Connection pooling for Azure operations
- File handles properly closed with context managers

### Testing Strategy
- Unit tests for individual components in `tests/unit/`
- Integration tests for end-to-end flows in `tests/integration/`
- Mock external dependencies (Kafka, Azure) in unit tests
- Use `scripts/test-kafka-producer.py` for manual Kafka testing

## API Endpoints

The service exposes a FastAPI-based REST API on port 8080:
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics
- `GET /storage/stats` - Storage statistics
- `POST /upload/trigger` - Trigger Azure upload
- `POST /cleanup/trigger` - Trigger cleanup
- `POST /storage/flush` - Flush buffers to disk
- `GET /azure/files` - List Azure files

API documentation is automatically available at `/docs` (Swagger UI).

## Monitoring Integration

The service exports Prometheus metrics and includes Grafana dashboards in `monitoring/grafana/dashboards/`. Key metrics include message counts, storage usage, upload success/failure rates, and cleanup statistics.