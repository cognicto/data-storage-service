# Sensor Data Storage Service

A high-performance, optimized microservice for consuming sensor data from Kafka, storing it in efficient hierarchical Parquet format, and managing lifecycle with Azure Blob Storage integration featuring comprehensive data quality monitoring.

## 🚀 Key Features

### Data Processing & Storage
- **Real-time Kafka Consumer**: Subscribes to sensor data topics using regex patterns with intelligent timestamp parsing
- **Optimized Storage**: Hierarchical organization with 80% reduction in file size through path-based metadata
- **Multi-level Aggregations**: Real-time minute, scheduled hourly, and daily aggregations with quality metrics
- **Data Quality Monitoring**: Comprehensive metrics tracking completeness, gaps, and statistical confidence

### Cloud Integration & Reliability  
- **Flexible Azure Authentication**: SAS token and storage account key support
- **Smart Upload Management**: Parallel uploads with retry logic, deduplication, and configurable intervals
- **Intelligent Cleanup**: Removes local files after successful upload with retention policies
- **Production Ready**: Docker support, Kubernetes manifests, monitoring with Prometheus/Grafana

### Monitoring & Operations
- **REST API**: Health monitoring, metrics, manual triggers, and file management endpoints
- **Comprehensive Metrics**: Data quality, upload success rates, storage usage, and performance metrics
- **Debug Tools**: Parquet file verification script with detailed analysis capabilities

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

### Optimized Data Storage Hierarchy

```
/data/raw/ (Optimized 2-column schema)
├── asset_001/
│   ├── 2026/04/04/14/
│   │   ├── hf_rms_1hz_ch1_20260404_14.parquet  (timestamp, value)
│   │   ├── hf_rms_1hz_ch2_20260404_14.parquet  (timestamp, value)
│   │   └── quad_ch1_20260404_14.parquet        (timestamp, value)

/data/aggregated/ (Enhanced with quality metrics)
├── asset_001/
│   ├── 2026/04/04/14/
│   │   ├── hf_rms_1hz_ch1_minute.parquet  (8 cols: stats + quality)
│   │   └── hf_rms_1hz_ch2_minute.parquet  (8 cols: stats + quality)
│   ├── 2026/04/04/
│   │   ├── hf_rms_1hz_ch1_hour.parquet    (8 cols: hourly stats)
│   │   └── hf_rms_1hz_ch2_hour.parquet    (8 cols: hourly stats)

/data/daily/ (Daily aggregations)
├── asset_001/
│   ├── 2026/04/
│   │   ├── hf_rms_1hz_ch1_day.parquet     (9 cols: daily stats)
│   │   └── hf_rms_1hz_ch2_day.parquet     (9 cols: daily stats)
```

### File Schema Comparison

| File Type | Columns | Size Reduction | Quality Metrics |
|-----------|---------|----------------|-----------------|
| **Raw** | 2 (timestamp, value) | 80% smaller | Asset/sensor in path |
| **Minute** | 8 (stats + quality) | 81% smaller | record_count, coverage |
| **Hourly** | 8 (aggregated stats) | 60% smaller | minute_count, coverage |
| **Daily** | 9 (multi-level stats) | 50% smaller | hour_count, full coverage |

## 🔧 Configuration

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_TOPIC_PATTERN` | Topic subscription pattern | `^sensor-data-.*` |
| `LOCAL_STORAGE_PATH` | Local storage directory | `/data/raw` |
| `CLEANUP_ENABLED` | Enable automatic cleanup | `true` |
| `CLEANUP_AGE_DAYS` | Days before cleanup | `7` |
| `UPLOAD_INTERVAL_SECONDS` | Upload frequency | `1800` (30 min) |

### Azure Authentication Options

**Option 1: SAS Token (Recommended)**
```bash
AZURE_BLOB_ENDPOINT=https://yourstorageaccount.blob.core.windows.net
AZURE_SAS_TOKEN=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-12-31T23:59:59Z...
AZURE_CONTAINER_NAME=sensor-data-cold-storage
```

**Option 2: Storage Account Key (Legacy)**
```bash
AZURE_STORAGE_ACCOUNT=yourstorageaccount
AZURE_STORAGE_KEY=your_storage_account_key
AZURE_CONTAINER_NAME=sensor-data-cold-storage
```

### Performance Tuning

| Variable | Description | Default |
|----------|-------------|---------|
| `MAX_ROWS_PER_FILE` | Records per Parquet file | `100000` |
| `AZURE_MAX_WORKERS` | Parallel upload threads | `4` |
| `PARQUET_COMPRESSION` | Compression algorithm | `snappy` |

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

## 🔧 Debug Tools

### Parquet File Analysis

Use the included verification script to analyze any Parquet file:

```bash
# Basic file analysis
python verify_parquet.py /path/to/file.parquet

# With sample data (recommended)
python verify_parquet.py /path/to/file.parquet --sample --sample-size 10

# Examples:
python verify_parquet.py /data/raw/asset_001/2026/04/04/14/hf_rms_1hz_ch1_20260404_14.parquet --sample
python verify_parquet.py /data/aggregated/asset_001/2026/04/04/14/hf_rms_1hz_ch1_minute.parquet --sample
```

**Analysis Features:**
- Record counts and file sizes
- Time range analysis and gap detection
- Data quality metrics and completeness
- Sample data display
- Column information and statistics
- Memory usage analysis

### Data Quality Monitoring

**Check aggregation completeness:**
```bash
# Via API
curl http://localhost:8080/storage/stats
curl http://localhost:8080/azure/files?prefix=aggregated/

# Via verification script
python verify_parquet.py /data/aggregated/*/2026/04/04/14/*_minute.parquet --sample
```

**Monitor data gaps:**
- `record_count` vs expected values (60 per minute for 1Hz sensors)
- `timestamp_start` to `timestamp_end` coverage analysis
- Time gaps > 2 seconds detection

## 🔍 Troubleshooting

### Common Issues

**Kafka Connection Failed**
```bash
# Check Kafka connectivity
kafkacat -L -b localhost:9092

# Verify topic exists
kafkacat -L -b localhost:9092 | grep sensor-data
```

**Azure Authentication Issues**
```bash
# Test SAS token authentication
python -c "from azure.storage.blob import BlobServiceClient; 
           client = BlobServiceClient(account_url='your_endpoint', credential='your_sas_token'); 
           print('Success!' if client.get_container_client('container').exists() else 'Failed')"
```

**Data Quality Issues**
- Check timestamp parsing with debug logging
- Verify sensor data format matches expected schema
- Use verification script to identify data gaps

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

### v2.0.0 (2026-04-04) - Major Storage Optimization
- **80% Storage Reduction**: Optimized schema with path-based metadata
- **Enhanced Azure Support**: SAS token authentication with automatic retry
- **Data Quality Monitoring**: Comprehensive metrics across all aggregation levels
- **Improved Aggregations**: Enhanced minute/hourly/daily with quality metrics
- **Debug Tools**: Parquet file verification script with detailed analysis
- **Smart Timestamp Parsing**: Handles Unix timestamps (ms/seconds) and ISO formats
- **Configurable Upload Intervals**: Adjustable upload frequency for testing/production

### v1.0.0 (2024-01-01) - Initial Release
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