# Kafka Connect Kubernetes Setup

This guide provides complete setup instructions for deploying Kafka Connect in your existing Kubernetes environment with a 3-pod Kafka cluster.

## 📋 Prerequisites

- ✅ Kubernetes cluster running with kubectl access
- ✅ Kafka cluster with 3 broker pods running
- ✅ TimescaleDB instance accessible from K8s cluster
- ✅ Basic knowledge of Kubernetes and Kafka Connect

## 🏗️ Architecture Overview

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   TimescaleDB       │    │   Kafka Connect     │    │   Kafka Cluster    │
│   (Data Source)     │───▶│   (Connector)       │───▶│   (3 Brokers)       │
│                     │    │                     │    │                     │
│ sensor_data table   │    │ JDBC Source         │    │ sensor-data-* topics│
└─────────────────────┘    │ Connector           │    └─────────────────────┘
                           └─────────────────────┘
```

## 🚀 Quick Start

### Step 1: Update Kafka Broker Names

First, check your actual Kafka service names:

```bash
# Check your Kafka pods and services
kubectl get pods | grep kafka
kubectl get svc | grep kafka
```

Update the broker names in the configuration files if they differ from:
- `kafka-broker-pod0:9092`
- `kafka-broker-pod1:9092` 
- `kafka-broker-pod2:9092`

### Step 2: Configure TimescaleDB Credentials

Edit the secret file with your actual credentials:

```bash
# Option 1: Edit the secret file directly
vim kubernetes/secret.yaml

# Option 2: Create secret from command line
kubectl create secret generic timescaledb-secret \
  --from-literal=username='your_db_user' \
  --from-literal=password='your_db_password' \
  --from-literal=jdbc-url='jdbc:postgresql://your-host:5432/your_db'
```

### Step 3: Deploy Kafka Connect

```bash
# Deploy with automatic connector download (recommended)
./scripts/deploy.sh --init-container

# OR deploy without init container (requires manual plugin setup)
./scripts/deploy.sh
```

### Step 4: Deploy TimescaleDB Connector

```bash
# Set environment variables for your TimescaleDB
export TIMESCALEDB_HOST="your-timescaledb-host"
export TIMESCALEDB_PORT="5432" 
export TIMESCALEDB_DATABASE="your_database_name"
export TIMESCALEDB_USER="your_username"
export TIMESCALEDB_PASSWORD="your_password"

# Deploy the connector
./scripts/connector-deploy.sh
```

### Step 5: Verify Setup

```bash
# Check status
./scripts/connector-status.sh

# Check logs
kubectl logs -f deployment/kafka-connect

# List Kafka topics
kubectl exec -it kafka-broker-pod0 -- kafka-topics --list --bootstrap-server localhost:9092
```

## 📁 File Structure

```
kafka-connect/
├── K8S-SETUP.md                           # This setup guide
├── README.md                              # Original Docker Compose guide
├── kubernetes/                            # K8s manifests
│   ├── configmap.yaml                     # Kafka Connect configuration
│   ├── deployment.yaml                    # Basic deployment
│   ├── init-container-deployment.yaml     # Deployment with auto-download
│   ├── plugins-configmap.yaml            # Plugin configuration
│   └── secret.yaml                       # Database credentials
├── scripts/                              # Management scripts
│   ├── deploy.sh                         # Deploy Kafka Connect
│   ├── connector-deploy.sh               # Deploy TimescaleDB connector
│   └── connector-status.sh               # Monitor and manage connectors
├── config/
│   └── timescaledb-source.json           # Updated connector config (K8s-ready)
└── properties/
    └── connect-distributed.properties     # Updated for 3-broker cluster
```

## 🔧 Configuration Details

### Kafka Connect Configuration

Key settings optimized for your 3-pod Kafka cluster:

```properties
# 3-broker cluster configuration
bootstrap.servers=kafka-broker-pod0:9092,kafka-broker-pod1:9092,kafka-broker-pod2:9092
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3
```

### TimescaleDB Connector Configuration

Enhanced connector configuration with:

```json
{
  "tasks.max": "3",                        # Utilize all 3 Kafka brokers
  "batch.max.rows": "1000",               # Optimized batch size
  "connection.attempts": "3",              # Connection resilience
  "errors.tolerance": "all",               # Error handling
  "transforms": "createKey,addTimestamp"   # Data transformation
}
```

## 🐛 Troubleshooting

### Common Issues

**1. Kafka Connect Pod Won't Start**
```bash
# Check pod events
kubectl describe pod -l app=kafka-connect

# Check logs
kubectl logs deployment/kafka-connect

# Common causes:
# - Wrong Kafka broker names
# - Network connectivity issues
# - Insufficient resources
```

**2. Connector Fails to Deploy**
```bash
# Check connector status
./scripts/connector-status.sh

# Check if plugins are loaded
curl http://localhost:8083/connector-plugins

# Common causes:
# - Missing JDBC driver
# - Wrong database credentials
# - Network access to TimescaleDB
```

**3. No Data Flowing**
```bash
# Check connector tasks
./scripts/connector-status.sh tasks

# Check Kafka topics
kubectl exec -it kafka-broker-pod0 -- kafka-topics --list --bootstrap-server localhost:9092

# Check TimescaleDB query
# Ensure your query returns data and timestamp column exists
```

### Debugging Commands

```bash
# Port-forward to Kafka Connect REST API
kubectl port-forward service/kafka-connect 8083:8083

# Access REST API directly
curl http://localhost:8083/
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/timescaledb-source-connector/status

# Check Kafka Connect logs
kubectl logs -f deployment/kafka-connect

# Check available plugins
curl http://localhost:8083/connector-plugins

# Restart connector
curl -X POST http://localhost:8083/connectors/timescaledb-source-connector/restart
```

## 🔧 Management Commands

### Connector Management

```bash
# Show connector status
./scripts/connector-status.sh status

# Show connector configuration
./scripts/connector-status.sh config

# List all connectors
./scripts/connector-status.sh list

# Restart connector
./scripts/connector-status.sh restart

# Pause connector
./scripts/connector-status.sh pause

# Resume connector
./scripts/connector-status.sh resume

# Delete connector
./scripts/connector-status.sh delete
```

### Cluster Management

```bash
# Show cluster information
./scripts/connector-status.sh cluster

# Show available plugins
./scripts/connector-status.sh plugins

# Scale Kafka Connect (edit deployment)
kubectl scale deployment kafka-connect --replicas=2
```

## 📊 Monitoring

### Health Checks

```bash
# Check pod health
kubectl get pods -l app=kafka-connect

# Check service endpoints
kubectl get endpoints kafka-connect

# Check resource usage
kubectl top pods -l app=kafka-connect
```

### Logs and Metrics

```bash
# Tail logs
kubectl logs -f deployment/kafka-connect

# Get recent logs
kubectl logs deployment/kafka-connect --tail=100

# Check connector lag (if metrics are enabled)
curl http://localhost:8083/metrics
```

## 🔐 Security Considerations

### Database Credentials

- ✅ Use Kubernetes Secrets for database passwords
- ✅ Enable SSL connections to TimescaleDB
- ✅ Use least-privilege database user
- ✅ Rotate credentials regularly

### Network Security

- ✅ Use NetworkPolicies to restrict access
- ✅ Enable TLS for Kafka Connect REST API
- ✅ Use service mesh for encryption in transit

## 🚀 Production Considerations

### High Availability

```bash
# Scale Kafka Connect for HA
kubectl scale deployment kafka-connect --replicas=2

# Use anti-affinity to spread across nodes
# (see deployment.yaml for pod anti-affinity example)
```

### Resource Limits

```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "2Gi"      # Adjust based on connector load
    cpu: "1000m"       # Adjust based on throughput needs
```

### Monitoring and Alerting

- 📊 Enable JMX metrics for Kafka Connect
- 🚨 Set up alerts for connector failures
- 📈 Monitor connector lag and throughput
- 🔍 Use distributed tracing for debugging

## 🎯 Next Steps

1. **Test Data Flow**: Verify data is flowing from TimescaleDB to Kafka topics
2. **Configure Sensor Storage Service**: Update your sensor storage service to consume from the new topics
3. **Set Up Monitoring**: Implement comprehensive monitoring and alerting
4. **Optimize Performance**: Tune connector settings based on your data volume
5. **Enable Security**: Implement proper authentication and encryption

## 📚 Additional Resources

- [Confluent Kafka Connect Documentation](https://docs.confluent.io/platform/current/connect/index.html)
- [JDBC Source Connector Configuration](https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/index.html)
- [TimescaleDB Documentation](https://docs.timescale.com/)
- [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)

## 💡 Tips for Success

1. **Start Small**: Begin with a simple connector configuration and gradually add complexity
2. **Monitor Resources**: Keep an eye on memory and CPU usage, especially during high-throughput periods
3. **Test Failure Scenarios**: Verify connector behavior during database outages and network issues
4. **Use Staging Environment**: Test connector configurations in staging before production deployment
5. **Document Changes**: Keep track of configuration changes and their impact on data flow