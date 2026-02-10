# Sensor Data Storage Service

A high-performance microservice for consuming sensor data from Kafka, storing it hierarchically in Parquet format, and managing lifecycle with Azure Blob Storage integration.

## 🚀 Features

- **Real-time Kafka Consumer**: Subscribes to sensor data topics using regex patterns
- **Hierarchical Storage**: Organizes data as `asset_id/yyyy/mm/dd/hh/sensor_name.parquet`
- **Azure Blob Integration**: Automatic upload to cold storage with retry logic
- **Smart Cleanup**: Removes local files after successful upload
- **Pre-computed Aggregations**: Minute, hourly, and daily aggregations for fast queries
- **REST API**: Health monitoring, metrics, and manual trigger endpoints
- **Production Ready**: Docker support, Kubernetes manifests, monitoring with Prometheus/Grafana

## 📋 Prerequisites

- Python 3.11+
- Kafka cluster (or use included docker-compose)
- Azure Storage Account (optional, for cloud storage)
- Docker & Docker Compose (for containerized deployment)

## 🛠️ Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/sensor-data-storage-service.git
cd sensor-data-storage-service
```

### 2. Setup Environment
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your configuration
vim .env

# Install dependencies
pip install -r requirements.txt
```

### 3. Run Locally
```bash
# Start the service
make run

# Or with development stack (includes Kafka)
make compose-dev
```

### 4. Run with Docker
```bash
# Build image
make docker-build

# Run container
make docker-run

# Or use docker-compose
make compose-up
```

## 📊 Architecture

```
Kafka Topics → Consumer → Storage Manager → Local Parquet Files
                               ↓
                        Aggregation Scheduler
                               ↓
                         Aggregated Files
                               ↓
                         Azure Uploader
                               ↓
                        Azure Blob Storage
                               ↓
                         Cleanup Service
```

### Data Storage Hierarchy

```
/data/raw/
├── asset_001/
│   ├── 2024/
│   │   ├── 01/
│   │   │   ├── 01/
│   │   │   │   ├── 00/
│   │   │   │   │   ├── sensor_temp.parquet
│   │   │   │   │   ├── sensor_pressure.parquet
│   │   │   │   │   └── sensor_humidity.parquet
│   │   │   │   ├── 01/
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── ...
│   └── ...
└── ...

/data/aggregated/
├── asset_001/
│   ├── 2024/01/01/00/sensor_temp_minute.parquet
│   ├── 2024/01/01/sensor_temp_hour.parquet
│   └── ...
└── ...
```

## 🔧 Configuration

Key environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_TOPIC_PATTERN` | Topic subscription pattern | `^sensor-data-.*` |
| `AZURE_STORAGE_ACCOUNT` | Azure account name | *(optional)* |
| `LOCAL_STORAGE_PATH` | Local storage directory | `/data/raw` |
| `CLEANUP_ENABLED` | Enable automatic cleanup | `true` |
| `CLEANUP_AGE_DAYS` | Days before cleanup | `7` |

See [.env.example](.env.example) for complete configuration options.

## 📡 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health check |
| `/metrics` | GET | Prometheus metrics |
| `/storage/stats` | GET | Storage statistics |
| `/upload/trigger` | POST | Trigger Azure upload |
| `/cleanup/trigger` | POST | Trigger cleanup |
| `/storage/flush` | POST | Flush buffers to disk |
| `/azure/files` | GET | List Azure files |

API documentation available at `http://localhost:8080/docs` (FastAPI automatic docs)

## 🧪 Testing

```bash
# Run unit tests
make test

# Run with coverage
pytest tests/ --cov=app --cov-report=html

# Test Kafka producer
make test-kafka
```

## 📦 Deployment

### Docker
```bash
# Build and push to registry
REGISTRY=your-registry make docker-push
```

### Kubernetes
```bash
# Deploy to cluster
make k8s-deploy

# Check logs
make k8s-logs

# Remove deployment
make k8s-delete
```

### Production Checklist
- [ ] Configure Azure credentials
- [ ] Set appropriate retention policies
- [ ] Configure monitoring alerts
- [ ] Set resource limits in Kubernetes
- [ ] Enable TLS for API endpoints
- [ ] Configure backup strategy

## 📈 Monitoring

The service exports Prometheus metrics and includes:
- Grafana dashboard configuration
- Alert rules for common issues
- Health check endpoints

Access monitoring:
- Metrics: `http://localhost:8080/metrics`
- Grafana: `http://localhost:3000` (when using docker-compose with monitoring profile)
- Prometheus: `http://localhost:9090`

### Key Metrics
- `messages_consumed_total` - Total Kafka messages consumed
- `storage_used_bytes` - Local storage usage
- `azure_upload_success_total` - Successful Azure uploads
- `azure_upload_failures_total` - Failed Azure uploads
- `cleanup_files_deleted_total` - Files cleaned up

## 🔍 Troubleshooting

### Common Issues

**Kafka Connection Failed**
```bash
# Check Kafka connectivity
kafkacat -L -b localhost:9092

# Verify topic exists
kafkacat -L -b localhost:9092 | grep sensor-data
```

**High Memory Usage**
- Reduce `MAX_ROWS_PER_FILE`
- Decrease `BUFFER_FLUSH_INTERVAL_SECONDS`
- Lower `AZURE_MAX_WORKERS`

**Upload Failures**
- Verify Azure credentials
- Check network connectivity
- Review retry configuration
- Check Azure container exists

**Storage Full**
- Decrease `CLEANUP_AGE_DAYS`
- Enable `CLEANUP_ENABLED`
- Increase upload frequency

## 📂 Project Structure

```
sensor-data-storage-service/
├── app/
│   ├── aggregation/    # Aggregation scheduler
│   ├── api/            # REST API routes
│   ├── azure/          # Azure blob uploader
│   ├── cleanup/        # File cleanup service
│   ├── kafka/          # Kafka consumer
│   ├── storage/        # Storage manager
│   ├── config.py       # Configuration
│   └── main.py         # Entry point
├── config/             # Configuration files
├── deployment/         # K8s manifests
├── docs/              # Documentation
├── monitoring/        # Prometheus/Grafana configs
├── scripts/           # Utility scripts
├── tests/             # Test suite
├── docker-compose.yml # Docker compose
├── Dockerfile         # Container definition
├── Makefile          # Build automation
└── requirements.txt   # Dependencies
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Write tests for new features
- Follow PEP 8 style guide
- Update documentation
- Add type hints
- Use semantic commit messages

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 🆘 Support

- Documentation: [docs/](docs/)
- Issues: [GitHub Issues](https://github.com/your-org/sensor-data-storage-service/issues)
- Slack: #sensor-storage-team
- Email: sensor-team@yourcompany.com

## 🔄 Changelog

### v1.0.0 (2024-01-01)
- Initial release
- Kafka consumer with regex topic patterns
- Hierarchical Parquet storage
- Azure Blob Storage integration
- Cleanup service
- Pre-computed aggregations
- REST API
- Docker and Kubernetes support

## 🙏 Acknowledgments

- Built with FastAPI, Confluent Kafka, and Azure SDK
- Monitoring with Prometheus and Grafana
- Containerization with Docker