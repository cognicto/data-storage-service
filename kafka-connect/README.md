# TimescaleDB Kafka Connect Setup

This guide explains how to set up the TimescaleDB source connector as a service using Docker Compose.

## Directory Structure

```
kafka-connect/
├── README.md
├── plugins/
│   └── timescaledb/           # Place your TimescaleDB connector JAR files here
├── config/
│   └── timescaledb-source.json # REST API connector configuration
└── properties/
    ├── connect-distributed.properties  # Kafka Connect worker configuration
    └── timescaledb-source.properties  # Properties format configuration
```

## Setup Steps

### 1. Copy Your Windows Files

Copy your files from Windows to the appropriate directories:

```bash
# Copy TimescaleDB connector JAR files
cp /path/to/your/timescaledb-connector.jar kafka-connect/plugins/timescaledb/

# Copy any dependency JARs
cp /path/to/your/postgresql-driver.jar kafka-connect/plugins/timescaledb/
cp /path/to/your/other-dependencies.jar kafka-connect/plugins/timescaledb/
```

### 2. Configure Database Connection

Edit `kafka-connect/config/timescaledb-source.json`:

```json
{
  "name": "timescaledb-source-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://your-timescaledb-host:5432/your_database",
    "connection.user": "your_username",
    "connection.password": "your_password",
    "query": "SELECT * FROM sensor_data WHERE timestamp > ? ORDER BY timestamp",
    "mode": "timestamp",
    "timestamp.column.name": "timestamp",
    "poll.interval.ms": "5000",
    "topic.prefix": "sensor-data-"
  }
}
```

### 3. Start the Service

```bash
# Start the development stack with Kafka Connect
make compose-dev

# Check if Kafka Connect is running
make kafka-connect-status
```

### 4. Deploy the Connector

```bash
# Deploy TimescaleDB connector
make timescaledb-deploy

# Check connector status
make timescaledb-status

# View logs
make kafka-connect-logs
```

## Management Commands

### Basic Operations
- `make timescaledb-deploy` - Deploy the connector
- `make timescaledb-status` - Check connector status
- `make timescaledb-restart` - Restart the connector
- `make timescaledb-delete` - Delete the connector

### Monitoring
- `make kafka-connect-status` - Check Kafka Connect cluster status
- `make kafka-connect-connectors` - List all connectors
- `make kafka-connect-logs` - View Kafka Connect logs

### Troubleshooting
- `make timescaledb-config` - View current connector configuration
- `make timescaledb-pause` - Pause connector
- `make timescaledb-resume` - Resume connector

## Configuration Options

### Common TimescaleDB Settings

```json
{
  "name": "timescaledb-source-connector",
  "config": {
    // Database connection
    "connection.url": "jdbc:postgresql://localhost:5432/sensors",
    "connection.user": "postgres",
    "connection.password": "password",
    
    // Query mode - use 'timestamp' for time-series data
    "mode": "timestamp",
    "timestamp.column.name": "created_at",
    "query": "SELECT sensor_id, value, created_at FROM measurements WHERE created_at > ? ORDER BY created_at",
    
    // Polling frequency
    "poll.interval.ms": "5000",
    
    // Topic naming
    "topic.prefix": "sensor-data-",
    
    // Batch size
    "batch.max.rows": "1000",
    
    // Error handling
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }
}
```

### Performance Tuning

For high-throughput scenarios:
- Increase `batch.max.rows`: 5000-10000
- Decrease `poll.interval.ms`: 1000-2000
- Use connection pooling
- Optimize your SQL query with proper indexes

### Security

- Use environment variables for sensitive data:
  ```bash
  export TIMESCALEDB_USER="your_username"
  export TIMESCALEDB_PASSWORD="your_password"
  ```
- Use SSL connections: `jdbc:postgresql://host:5432/db?ssl=true`
- Restrict database user permissions

## Troubleshooting

### Common Issues

1. **Connector fails to start**
   ```bash
   make kafka-connect-logs
   # Check for missing JAR files or classpath issues
   ```

2. **Database connection issues**
   ```bash
   make timescaledb-status
   # Check connection URL, credentials, and network access
   ```

3. **No data flowing**
   ```bash
   # Check if query returns data
   make timescaledb-config
   # Verify timestamp column and query syntax
   ```

4. **Performance issues**
   - Monitor connector lag
   - Check database query performance
   - Adjust polling interval and batch size

### Logs Location

- Kafka Connect logs: `docker-compose logs kafka-connect`
- Connector-specific logs: Check the Kafka Connect REST API for task logs

## Migration from Command Line

If you were running Kafka Connect standalone from command line:

1. Your `connect-standalone.properties` maps to `connect-distributed.properties`
2. Your `timescale-source.properties` maps to `timescaledb-source.json`
3. JAR files go in `kafka-connect/plugins/timescaledb/`
4. Use REST API instead of property files for deployment