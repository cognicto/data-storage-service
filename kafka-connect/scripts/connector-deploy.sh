#!/bin/bash
# TimescaleDB Connector Deployment Script for K8s

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="${KAFKA_CONNECT_NAMESPACE:-default}"
KAFKA_CONNECT_SERVICE="kafka-connect"
KAFKA_CONNECT_PORT="8083"
CONNECTOR_NAME="timescaledb-source-connector"
CONFIG_FILE="config/timescaledb-source.json"

echo -e "${BLUE}🔌 Deploying TimescaleDB Source Connector...${NC}"

# Function to check if Kafka Connect is running
check_kafka_connect() {
    echo -e "${BLUE}🔍 Checking Kafka Connect status...${NC}"
    
    # Check if Kafka Connect pod is running
    if ! kubectl get pods -l app=kafka-connect -n $NAMESPACE | grep -q "Running"; then
        echo -e "${RED}❌ Kafka Connect is not running. Please deploy it first.${NC}"
        echo -e "${YELLOW}💡 Run: ./scripts/deploy.sh${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Kafka Connect is running${NC}"
}

# Function to create port-forward for API access
create_port_forward() {
    echo -e "${BLUE}🌐 Creating port-forward to Kafka Connect...${NC}"
    
    # Kill any existing port-forwards
    pkill -f "kubectl port-forward.*kafka-connect" 2>/dev/null || true
    
    # Create new port-forward
    kubectl port-forward service/$KAFKA_CONNECT_SERVICE $KAFKA_CONNECT_PORT:$KAFKA_CONNECT_PORT -n $NAMESPACE &
    PF_PID=$!
    
    # Wait for port-forward to be established
    sleep 3
    
    # Test connection
    if ! curl -s http://localhost:$KAFKA_CONNECT_PORT/ > /dev/null; then
        echo -e "${RED}❌ Failed to connect to Kafka Connect REST API${NC}"
        kill $PF_PID 2>/dev/null || true
        exit 1
    fi
    
    echo -e "${GREEN}✅ Port-forward established${NC}"
}

# Function to check if connector already exists
check_existing_connector() {
    echo -e "${BLUE}🔍 Checking for existing connector...${NC}"
    
    if curl -s http://localhost:$KAFKA_CONNECT_PORT/connectors | grep -q "\"$CONNECTOR_NAME\""; then
        echo -e "${YELLOW}⚠️  Connector '$CONNECTOR_NAME' already exists${NC}"
        read -p "Do you want to update it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${BLUE}🔄 Deleting existing connector...${NC}"
            curl -X DELETE http://localhost:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME
            sleep 2
        else
            echo -e "${YELLOW}Skipping deployment${NC}"
            cleanup_and_exit 0
        fi
    fi
}

# Function to validate config file
validate_config() {
    if [ ! -f "$CONFIG_FILE" ]; then
        echo -e "${RED}❌ Configuration file not found: $CONFIG_FILE${NC}"
        exit 1
    fi
    
    # Check if config is valid JSON
    if ! cat "$CONFIG_FILE" | python3 -m json.tool > /dev/null 2>&1; then
        echo -e "${RED}❌ Configuration file contains invalid JSON${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Configuration file is valid${NC}"
}

# Function to substitute environment variables in config
substitute_env_vars() {
    echo -e "${BLUE}🔧 Substituting environment variables in config...${NC}"
    
    # Create temporary config with environment variables substituted
    TEMP_CONFIG=$(mktemp)
    
    # Read environment variables
    TIMESCALEDB_HOST="${TIMESCALEDB_HOST:-your-timescaledb-host}"
    TIMESCALEDB_PORT="${TIMESCALEDB_PORT:-5432}"
    TIMESCALEDB_DATABASE="${TIMESCALEDB_DATABASE:-your_database}"
    TIMESCALEDB_USER="${TIMESCALEDB_USER:-your_username}"
    TIMESCALEDB_PASSWORD="${TIMESCALEDB_PASSWORD:-your_password}"
    
    # Build JDBC URL
    JDBC_URL="jdbc:postgresql://${TIMESCALEDB_HOST}:${TIMESCALEDB_PORT}/${TIMESCALEDB_DATABASE}"
    
    # Substitute variables in config
    sed "s|\${env:TIMESCALEDB_JDBC_URL:[^}]*}|$JDBC_URL|g; \
         s|\${env:TIMESCALEDB_USER:[^}]*}|$TIMESCALEDB_USER|g; \
         s|\${env:TIMESCALEDB_PASSWORD:[^}]*}|$TIMESCALEDB_PASSWORD|g" \
         "$CONFIG_FILE" > "$TEMP_CONFIG"
    
    echo -e "${GREEN}✅ Environment variables substituted${NC}"
    echo "$TEMP_CONFIG"
}

# Function to deploy connector
deploy_connector() {
    echo -e "${BLUE}🚀 Deploying connector...${NC}"
    
    TEMP_CONFIG=$(substitute_env_vars)
    
    # Deploy the connector
    RESPONSE=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        --data @"$TEMP_CONFIG" \
        http://localhost:$KAFKA_CONNECT_PORT/connectors)
    
    # Clean up temp file
    rm -f "$TEMP_CONFIG"
    
    # Check if deployment was successful
    if echo "$RESPONSE" | grep -q "error"; then
        echo -e "${RED}❌ Failed to deploy connector:${NC}"
        echo "$RESPONSE" | python3 -m json.tool
        exit 1
    fi
    
    echo -e "${GREEN}✅ Connector deployed successfully!${NC}"
    echo "$RESPONSE" | python3 -m json.tool
}

# Function to check connector status
check_connector_status() {
    echo -e "${BLUE}📊 Checking connector status...${NC}"
    
    # Wait a moment for connector to initialize
    sleep 3
    
    STATUS_RESPONSE=$(curl -s http://localhost:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME/status)
    
    if echo "$STATUS_RESPONSE" | grep -q '"state":"RUNNING"'; then
        echo -e "${GREEN}✅ Connector is running successfully!${NC}"
    else
        echo -e "${YELLOW}⚠️  Connector status:${NC}"
        echo "$STATUS_RESPONSE" | python3 -m json.tool
    fi
    
    # Show task status
    echo -e "\n${BLUE}📋 Task Status:${NC}"
    echo "$STATUS_RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for task in data.get('tasks', []):
    state = task.get('state', 'UNKNOWN')
    worker_id = task.get('worker_id', 'unknown')
    print(f'  Task {task.get(\"id\", \"unknown\")}: {state} on {worker_id}')
    if 'trace' in task:
        print(f'    Error: {task.get(\"trace\", \"No details\")}')
"
}

# Function to show next steps
show_next_steps() {
    echo -e "\n${GREEN}🎉 TimescaleDB Connector deployed successfully!${NC}"
    echo -e "\n${BLUE}📋 Next Steps:${NC}"
    echo -e "1. Check connector status:"
    echo -e "   ${YELLOW}./scripts/connector-status.sh${NC}"
    echo -e ""
    echo -e "2. Monitor connector logs:"
    echo -e "   ${YELLOW}kubectl logs -f deployment/kafka-connect -n $NAMESPACE${NC}"
    echo -e ""
    echo -e "3. Check Kafka topics:"
    echo -e "   ${YELLOW}kubectl exec -it kafka-broker-pod0 -n $NAMESPACE -- kafka-topics --list --bootstrap-server localhost:9092${NC}"
    echo -e ""
    echo -e "4. Pause/Resume connector:"
    echo -e "   ${YELLOW}curl -X PUT http://localhost:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME/pause${NC}"
    echo -e "   ${YELLOW}curl -X PUT http://localhost:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME/resume${NC}"
    echo -e ""
    echo -e "5. Delete connector:"
    echo -e "   ${YELLOW}curl -X DELETE http://localhost:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_NAME${NC}"
}

# Cleanup function
cleanup_and_exit() {
    if [ ! -z "$PF_PID" ]; then
        kill $PF_PID 2>/dev/null || true
    fi
    exit ${1:-0}
}

# Trap to cleanup on exit
trap cleanup_and_exit SIGINT SIGTERM

# Main execution
main() {
    check_kafka_connect
    validate_config
    create_port_forward
    check_existing_connector
    deploy_connector
    check_connector_status
    show_next_steps
    cleanup_and_exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config|-c)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --connector-name|-n)
            CONNECTOR_NAME="$2"
            shift 2
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --config, -c           Config file path (default: config/timescaledb-source.json)"
            echo "  --connector-name, -n   Connector name (default: timescaledb-source-connector)"
            echo "  --namespace           Kubernetes namespace (default: default)"
            echo "  --help, -h            Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  TIMESCALEDB_HOST      TimescaleDB host"
            echo "  TIMESCALEDB_PORT      TimescaleDB port (default: 5432)"
            echo "  TIMESCALEDB_DATABASE  Database name"
            echo "  TIMESCALEDB_USER      Database username"
            echo "  TIMESCALEDB_PASSWORD  Database password"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Run main function
main