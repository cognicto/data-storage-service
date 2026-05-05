# ADLS Gen2 Setup Guide
## Azure Data Lake Storage Gen2 Configuration

This guide walks you through setting up ADLS Gen2 for the Sensor Data Storage Service.

---

## 🔧 Quick Setup

### 1. **Your Connection String**
Based on your provided credentials, here's the correct format:

```bash
# Your ADLS Gen2 Configuration
AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net"
AZURE_FILE_SYSTEM_NAME=sensor-data-cold-storage
AZURE_USE_ADLS_GEN2=true
```

**Note:** I corrected the typos in your original string:
- `DefaultEndpoimtProtocol` → `DefaultEndpointsProtocol`
- `AccontKey` → `AccountKey`

### 2. **Complete Environment Configuration**
Create a `.env` file with these settings:

```bash
# Copy from .env.adls-gen2.example and update:
cp .env.adls-gen2.example .env

# Edit with your actual values:
nano .env
```

**Your `.env` file should contain:**
```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_PATTERN=^sensor-data-quad.*
KAFKA_CONSUMER_GROUP=sensor-storage-service

# ADLS Gen2 Configuration
AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net"
AZURE_FILE_SYSTEM_NAME=sensor-data-cold-storage
AZURE_USE_ADLS_GEN2=true

# Storage Configuration
LOCAL_STORAGE_PATH=/data/raw
PARQUET_COMPRESSION=lz4

# Service Configuration
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8080
LOG_LEVEL=INFO

# Performance Settings
AZURE_MAX_RETRIES=3
AZURE_MAX_WORKERS=4
UPLOAD_INTERVAL_SECONDS=1800
```

---

## 🚀 Step-by-Step Setup

### Step 1: Verify Azure Storage Account
First, ensure your Azure Storage Account has ADLS Gen2 enabled:

```bash
# Check if hierarchical namespace is enabled
az storage account show \
  --name sensedatalaketest \
  --query "isHnsEnabled"
```

**Expected output:** `true`

If not enabled, you need to enable it (⚠️ **irreversible operation**):
```bash
# Enable hierarchical namespace (ADLS Gen2)
az storage account update \
  --name sensedatalaketest \
  --enable-hierarchical-namespace true
```

### Step 2: Create File System (Container)
Create the file system that will store your sensor data:

```bash
# Using Azure CLI
az storage fs create \
  --name sensor-data-cold-storage \
  --connection-string "DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net"
```

**Or using Azure Portal:**
1. Navigate to your storage account `sensedatalaketest`
2. Go to "Data Lake Storage" → "File systems"
3. Click "+ File system"
4. Name: `sensor-data-cold-storage`
5. Click "Create"

### Step 3: Install Dependencies
Make sure you have the required Azure libraries:

```bash
# Install ADLS Gen2 Python SDK
pip install azure-storage-file-datalake==12.21.0

# Or install all requirements
pip install -r requirements.txt
```

### Step 4: Test Connection
Test your ADLS Gen2 connection:

```bash
# Start the service
make run

# Check health endpoint
curl http://localhost:8080/health

# Look for ADLS Gen2 confirmation in logs
# Expected: "Using ADLS Gen2 uploader"
# Expected: "Connected to ADLS Gen2 file system 'sensor-data-cold-storage'"
```

### Step 5: Verify File System Operations
Test file operations:

```bash
# Trigger a manual upload test
curl -X POST http://localhost:8080/upload/trigger

# List files to verify ADLS Gen2 integration
curl http://localhost:8080/azure/files

# Expected response should include "storage_type": "ADLS Gen2"
```

---

## 📁 File System Structure

Your ADLS Gen2 file system will organize data hierarchically:

```
sensor-data-cold-storage/  (File System)
├── asset_001/
│   ├── 2026/
│   │   ├── 04/
│   │   │   ├── 04/
│   │   │   │   ├── 14/
│   │   │   │   │   ├── quad_ch1_20260404_14.parquet
│   │   │   │   │   └── temp_sensor_20260404_14.parquet
├── aggregated/
│   ├── asset_001/
│   │   ├── 2026/04/04/14/
│   │   │   ├── quad_ch1_minute.parquet
│   │   │   └── temp_sensor_minute.parquet
├── daily/
│   ├── asset_001/
│   │   ├── 2026/04/
│   │   │   ├── quad_ch1_day.parquet
│   │   │   └── temp_sensor_day.parquet
```

**Benefits of ADLS Gen2 hierarchical structure:**
- **Directory operations**: Create, list, delete directories natively
- **POSIX compliance**: Standard file system operations
- **Better performance**: 30% faster than blob storage for analytics
- **ACL support**: Fine-grained access control

---

## 🔍 Troubleshooting

### Common Issues & Solutions

#### 1. **Connection String Format Error**
**Error:** `Invalid connection string format`

**Solution:** Verify connection string format:
```bash
# Correct format:
AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net"

# Common mistakes:
# - Missing quotes around the entire string
# - Typos: "DefaultEndpoimtProtocol" or "AccontKey"
# - Missing semicolons between parameters
```

#### 2. **File System Not Found**
**Error:** `The specified filesystem does not exist`

**Solution:** Create the file system:
```bash
az storage fs create \
  --name sensor-data-cold-storage \
  --connection-string "your_connection_string_here"
```

#### 3. **Permission Denied**
**Error:** `This request is not authorized to perform this operation`

**Solution:** Verify account key and permissions:
```bash
# Test with Azure CLI
az storage account show \
  --name sensedatalaketest \
  --query "primaryEndpoints"
```

#### 4. **Hierarchical Namespace Not Enabled**
**Error:** `The specified account kind does not support this operation`

**Solution:** Enable ADLS Gen2 on your storage account:
```bash
az storage account update \
  --name sensedatalaketest \
  --enable-hierarchical-namespace true
```

#### 5. **Service Using Blob Storage Instead of ADLS Gen2**
**Logs:** `"Using legacy Blob Storage uploader"`

**Solution:** Check configuration:
```bash
# Ensure these are set correctly:
AZURE_USE_ADLS_GEN2=true
AZURE_CONNECTION_STRING="your_connection_string"
AZURE_FILE_SYSTEM_NAME=sensor-data-cold-storage

# Restart service after configuration change
make run
```

---

## 🧪 Testing Your Setup

### 1. **Basic Connection Test**
```bash
# Python test script
python3 -c "
from azure.storage.filedatalake import DataLakeServiceClient

conn_str = 'DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net'
client = DataLakeServiceClient.from_connection_string(conn_str)

try:
    fs_client = client.get_file_system_client('sensor-data-cold-storage')
    properties = fs_client.get_file_system_properties()
    print(f'✅ ADLS Gen2 connection successful!')
    print(f'File system: sensor-data-cold-storage')
    print(f'Last modified: {properties.last_modified}')
except Exception as e:
    print(f'❌ Connection failed: {e}')
"
```

### 2. **Service Integration Test**
```bash
# Start service and check logs
make run 2>&1 | grep -i adls

# Expected output:
# "Using ADLS Gen2 uploader"
# "Connected to ADLS Gen2 file system 'sensor-data-cold-storage'"
# "ADLS Gen2 connection test successful"
```

### 3. **File Upload Test**
```bash
# Create test data and upload
echo '{"sensor": "test", "value": 123}' > /tmp/test.json

# Trigger upload
curl -X POST http://localhost:8080/upload/trigger

# Check if files appear in ADLS Gen2
curl http://localhost:8080/azure/files | jq '.storage_type'
# Expected: "ADLS Gen2"
```

---

## 🔄 Migration from Blob Storage

If you're migrating from existing blob storage:

### 1. **Backup Current Configuration**
```bash
# Save current .env file
cp .env .env.blob.backup
```

### 2. **Enable Hierarchical Namespace**
```bash
# This is irreversible - ensure you have backups
az storage account update \
  --name sensedatalaketest \
  --enable-hierarchical-namespace true
```

### 3. **Update Configuration**
```bash
# Update .env file with ADLS Gen2 settings
AZURE_USE_ADLS_GEN2=true
AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net"
AZURE_FILE_SYSTEM_NAME=sensor-data-cold-storage  # Same as your container name
```

### 4. **Verify Data Accessibility**
```bash
# Restart service
make run

# Check that existing data is still accessible
curl http://localhost:8080/azure/files
```

**Note:** ADLS Gen2 maintains 100% backward compatibility with blob storage APIs, so existing data remains accessible.

---

## 🎯 Next Steps

After successful setup:

1. **Monitor Performance**: Check `/metrics` endpoint for ADLS Gen2 performance
2. **Set Up Monitoring**: Configure alerts for upload failures
3. **Optimize Settings**: Tune `AZURE_MAX_WORKERS` based on your network bandwidth
4. **Configure Retention**: Adjust `CLEANUP_AGE_DAYS` for your data retention needs
5. **Enable Analytics**: Set up Azure Synapse or Databricks for advanced analytics

---

## 📞 Support

If you encounter issues:

1. **Check Service Logs**: `docker logs sensor-storage-service`
2. **Test Azure CLI**: `az storage account show --name sensedatalaketest`
3. **Verify Permissions**: Ensure account key has full storage access
4. **Review Configuration**: Double-check connection string format

**Common Configuration Template:**
```bash
# Complete working configuration for sensedatalaketest
AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net"
AZURE_FILE_SYSTEM_NAME=sensor-data-cold-storage
AZURE_USE_ADLS_GEN2=true
AZURE_MAX_RETRIES=3
AZURE_MAX_WORKERS=4
```

Your setup should now be ready for ADLS Gen2! 🚀