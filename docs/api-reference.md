# API Reference

## Base URL
```
http://localhost:8080
```

## Authentication
Currently, no authentication is required. In production, implement API key or OAuth2.

## Endpoints

### Health Check

#### GET /health
Returns the health status of all service components.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00Z",
  "components": {
    "kafka": {
      "healthy": true,
      "metrics": {
        "messages_consumed": 10000,
        "last_message_time": "2024-01-01T00:00:00Z"
      }
    },
    "storage": {
      "healthy": true,
      "stats": {
        "total_files": 100,
        "total_size_mb": 500
      }
    },
    "azure": {
      "healthy": true,
      "metrics": {
        "azure_configured": true,
        "container_name": "sensor-data-cold-storage"
      }
    },
    "cleanup": {
      "healthy": true,
      "metrics": {
        "is_running": true,
        "total_files_deleted": 50
      }
    }
  }
}
```

### Metrics

#### GET /metrics
Returns detailed service metrics.

**Response:**
```json
{
  "kafka": {
    "messages_consumed": 10000,
    "messages_failed": 10,
    "last_message_time": "2024-01-01T00:00:00Z",
    "is_running": true
  },
  "storage": {
    "total_files": 100,
    "total_size_bytes": 524288000,
    "total_size_mb": 500,
    "assets": {
      "asset_001": {
        "files": 50,
        "size_bytes": 262144000
      }
    },
    "buffer_count": 5,
    "buffered_records": 1000
  },
  "azure": {
    "upload_stats": {
      "total_uploads": 100,
      "successful_uploads": 95,
      "failed_uploads": 5,
      "total_bytes": 524288000
    }
  },
  "cleanup": {
    "cleanup_stats": {
      "total_files_deleted": 50,
      "total_bytes_freed": 262144000,
      "cleanup_runs": 10
    }
  }
}
```

### Upload Operations

#### POST /upload/trigger
Manually triggers upload of all local files to Azure.

**Response:**
```json
{
  "status": "started",
  "message": "Upload of 100 files started in background",
  "files_to_process": 100
}
```

### Cleanup Operations

#### POST /cleanup/trigger
Manually triggers cleanup of uploaded files.

**Response:**
```json
{
  "status": "started",
  "message": "Cleanup started in background"
}
```

### Storage Operations

#### POST /storage/flush
Flushes all in-memory buffers to disk.

**Response:**
```json
{
  "status": "completed",
  "files_flushed": 5,
  "files": [
    "/data/raw/asset_001/2024/01/01/00/sensor_001.parquet"
  ]
}
```

#### GET /storage/stats
Returns detailed storage statistics.

**Response:**
```json
{
  "total_files": 100,
  "total_size_bytes": 524288000,
  "total_size_mb": 500,
  "assets": {
    "asset_001": {
      "files": 50,
      "size_bytes": 262144000
    }
  },
  "buffer_count": 5,
  "buffered_records": 1000,
  "minute_buffer_count": 3,
  "minute_buffered_records": 180
}
```

### Azure Operations

#### GET /azure/files
Lists files in Azure Blob Storage.

**Query Parameters:**
- `prefix` (optional): Filter files by prefix

**Response:**
```json
{
  "files": [
    {
      "name": "asset_001/2024/01/01/00/sensor_001.parquet",
      "size_mb": 5.2,
      "last_modified": "2024-01-01T00:00:00Z",
      "content_type": "application/octet-stream"
    }
  ],
  "count": 100,
  "total_size_mb": 500
}
```

## Error Responses

All endpoints return appropriate HTTP status codes:

- `200 OK`: Successful operation
- `400 Bad Request`: Invalid request parameters
- `500 Internal Server Error`: Server error
- `503 Service Unavailable`: Service component not available

Error response format:
```json
{
  "detail": "Error message describing what went wrong"
}
```

## Rate Limiting

In production, rate limiting is configured at:
- 1000 requests per minute per IP address
- Configurable via `API_RATE_LIMIT` environment variable

## WebSocket Support

Future versions will support WebSocket connections for real-time metrics:
- `/ws/metrics`: Real-time metrics stream
- `/ws/logs`: Real-time log stream