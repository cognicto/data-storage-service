# Deployment Guide

This guide covers different deployment options for the Sensor Data Storage Service.

## Quick Start - Docker Compose

### Development Environment
```bash
# Copy and edit environment file
cp .env.example .env
# Edit .env with your configuration

# Start development environment (includes Kafka)
./scripts/start-dev.sh

# Or manually:
docker-compose --profile dev up -d
```

### Production Environment
```bash
# Ensure .env is configured for production
./scripts/start-prod.sh

# Or manually:
docker-compose up -d
```

## Kubernetes Deployment

### Prerequisites
- Kubernetes cluster (v1.20+)
- kubectl configured
- Azure Storage Account credentials
- Kafka cluster (external or in-cluster)

### Step 1: Create Namespace
```bash
kubectl apply -f deployment/kubernetes/namespace.yaml
```

### Step 2: Configure Secrets
Edit `deployment/kubernetes/secret.yaml` with your actual credentials:
```bash
kubectl apply -f deployment/kubernetes/secret.yaml
```

### Step 3: Apply Configuration
```bash
kubectl apply -f deployment/kubernetes/configmap.yaml
```

### Step 4: Deploy Service
```bash
kubectl apply -f deployment/kubernetes/deployment.yaml
```

### Step 5: Verify Deployment
```bash
# Check pods
kubectl get pods -n sensor-storage

# Check service health
kubectl port-forward svc/sensor-storage-service 8080:8080 -n sensor-storage
curl http://localhost:8080/health
```

## Azure Container Instances

### Deploy with Azure CLI
```bash
# Create resource group
az group create --name sensor-storage-rg --location eastus

# Create storage account for data persistence
az storage account create \
  --resource-group sensor-storage-rg \
  --name sensorstoragedata \
  --location eastus \
  --sku Standard_LRS

# Create file share
az storage share create \
  --account-name sensorstoragedata \
  --name sensor-data

# Deploy container
az container create \
  --resource-group sensor-storage-rg \
  --name sensor-storage-service \
  --image your-registry/sensor-storage-service:latest \
  --cpu 1 \
  --memory 2 \
  --ports 8080 \
  --environment-variables \
    KAFKA_BOOTSTRAP_SERVERS="your-kafka-servers" \
    AZURE_STORAGE_ACCOUNT="your-storage-account" \
    LOCAL_STORAGE_PATH="/data/raw" \
  --secure-environment-variables \
    AZURE_STORAGE_KEY="your-storage-key" \
  --azure-file-volume-account-name sensorstoragedata \
  --azure-file-volume-account-key "$(az storage account keys list --resource-group sensor-storage-rg --account-name sensorstoragedata --query [0].value -o tsv)" \
  --azure-file-volume-share-name sensor-data \
  --azure-file-volume-mount-path /data
```

## AWS ECS Deployment

### Using ECS Fargate
```bash
# Create task definition
aws ecs register-task-definition --cli-input-json file://deployment/aws/task-definition.json

# Create service
aws ecs create-service \
  --cluster your-cluster \
  --service-name sensor-storage-service \
  --task-definition sensor-storage-service \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-12345],securityGroups=[sg-12345],assignPublicIp=ENABLED}"
```

## Monitoring and Observability

### Prometheus + Grafana (Docker Compose)
```bash
# Start with monitoring profile
docker-compose --profile monitoring up -d

# Access Grafana
open http://localhost:3000
# Login: admin/admin
```

### Custom Metrics
The service exposes metrics at `/metrics` endpoint:
- Kafka consumption metrics
- Storage statistics
- Azure upload metrics
- File cleanup metrics

### Logging
Logs are structured JSON and can be ingested by:
- ELK Stack
- Splunk
- Azure Monitor
- AWS CloudWatch

### Health Checks
- `/health` - Overall service health
- Component-specific health checks included

## Scaling Considerations

### Horizontal Scaling
- Multiple instances can run with same consumer group
- Kafka will automatically distribute partitions
- Use sticky sessions if needed for API

### Vertical Scaling
- Increase memory for larger file buffers
- Increase CPU for faster Parquet processing
- Increase disk for local storage

### Performance Tuning
```env
# High-throughput configuration
KAFKA_MAX_POLL_RECORDS=1000
MAX_ROWS_PER_FILE=500000
AZURE_MAX_WORKERS=8
PARQUET_COMPRESSION=snappy  # Faster than lz4
```

## Troubleshooting

### Common Issues

**1. Service won't start**
```bash
# Check logs
docker-compose logs sensor-storage-service
kubectl logs -f deployment/sensor-storage-service -n sensor-storage
```

**2. Kafka connection issues**
```bash
# Test Kafka connectivity
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

**3. Azure upload failures**
```bash
# Check Azure credentials
curl -X POST http://localhost:8080/upload/trigger
# Check response and logs
```

**4. High disk usage**
```bash
# Check cleanup configuration
curl http://localhost:8080/metrics | grep cleanup
# Manually trigger cleanup
curl -X POST http://localhost:8080/cleanup/trigger
```

### Performance Issues

**1. Slow Kafka consumption**
- Check `KAFKA_MAX_POLL_RECORDS`
- Verify consumer group lag
- Check Kafka cluster health

**2. Slow file writes**
- Check disk I/O performance
- Adjust `MAX_ROWS_PER_FILE`
- Consider different compression algorithm

**3. Azure upload bottlenecks**
- Increase `AZURE_MAX_WORKERS`
- Check network bandwidth
- Verify Azure region proximity

## Security Considerations

### Container Security
- Non-root user in container
- Read-only filesystem where possible
- Minimal base image (python:slim)

### Network Security
- Use TLS for Kafka connections
- Secure Azure storage keys
- Network policies in Kubernetes

### Data Security
- Encryption at rest (Azure Storage)
- Encryption in transit (HTTPS/TLS)
- Access controls and auditing