#!/bin/bash
# Kafka Connect K8s Deployment Script

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

echo -e "${BLUE}🚀 Deploying Kafka Connect to Kubernetes...${NC}"

# Function to check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        echo -e "${RED}❌ kubectl not found. Please install kubectl first.${NC}"
        exit 1
    fi
}

# Function to check if Kafka cluster is running
check_kafka_cluster() {
    echo -e "${BLUE}🔍 Checking Kafka cluster status...${NC}"
    
    # Check for Kafka pods
    KAFKA_PODS=$(kubectl get pods -n $NAMESPACE | grep kafka-broker || true)
    if [ -z "$KAFKA_PODS" ]; then
        echo -e "${RED}❌ No Kafka broker pods found. Please ensure your Kafka cluster is running.${NC}"
        echo -e "${YELLOW}💡 Expected pods: kafka-broker-pod0, kafka-broker-pod1, kafka-broker-pod2${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Found Kafka broker pods${NC}"
    echo "$KAFKA_PODS"
}

# Function to deploy Kafka Connect
deploy_kafka_connect() {
    echo -e "${BLUE}📦 Deploying Kafka Connect components...${NC}"
    
    # Apply configurations in order
    kubectl apply -f kubernetes/secret.yaml -n $NAMESPACE
    echo -e "${GREEN}✅ Applied secrets${NC}"
    
    kubectl apply -f kubernetes/configmap.yaml -n $NAMESPACE
    kubectl apply -f kubernetes/plugins-configmap.yaml -n $NAMESPACE
    echo -e "${GREEN}✅ Applied ConfigMaps${NC}"
    
    # Choose deployment type based on whether init container is needed
    if [ "$USE_INIT_CONTAINER" = "true" ]; then
        kubectl apply -f kubernetes/init-container-deployment.yaml -n $NAMESPACE
        echo -e "${GREEN}✅ Applied Kafka Connect deployment with init container${NC}"
    else
        kubectl apply -f kubernetes/deployment.yaml -n $NAMESPACE
        echo -e "${GREEN}✅ Applied Kafka Connect deployment${NC}"
    fi
    
    echo -e "${GREEN}✅ Kafka Connect deployment completed${NC}"
}

# Function to wait for Kafka Connect to be ready
wait_for_kafka_connect() {
    echo -e "${BLUE}⏳ Waiting for Kafka Connect to be ready...${NC}"
    
    # Wait for deployment to be available
    kubectl rollout status deployment/kafka-connect -n $NAMESPACE --timeout=300s
    
    # Wait for service to be ready
    echo -e "${BLUE}🔍 Checking Kafka Connect REST API...${NC}"
    
    # Port-forward to check health
    kubectl port-forward service/$KAFKA_CONNECT_SERVICE $KAFKA_CONNECT_PORT:$KAFKA_CONNECT_PORT -n $NAMESPACE &
    PF_PID=$!
    
    # Wait for port-forward to be established
    sleep 5
    
    # Check health endpoint
    for i in {1..30}; do
        if curl -s http://localhost:$KAFKA_CONNECT_PORT/ > /dev/null; then
            echo -e "${GREEN}✅ Kafka Connect is ready!${NC}"
            kill $PF_PID 2>/dev/null || true
            return 0
        fi
        echo -e "${YELLOW}⏳ Waiting for Kafka Connect... (attempt $i/30)${NC}"
        sleep 10
    done
    
    kill $PF_PID 2>/dev/null || true
    echo -e "${RED}❌ Kafka Connect failed to start within timeout${NC}"
    exit 1
}

# Function to show deployment status
show_status() {
    echo -e "${BLUE}📊 Kafka Connect Status:${NC}"
    
    echo -e "\n${BLUE}Pods:${NC}"
    kubectl get pods -l app=kafka-connect -n $NAMESPACE
    
    echo -e "\n${BLUE}Services:${NC}"
    kubectl get services -l app=kafka-connect -n $NAMESPACE
    
    echo -e "\n${BLUE}ConfigMaps:${NC}"
    kubectl get configmaps kafka-connect-config -n $NAMESPACE 2>/dev/null || echo "ConfigMap not found"
    
    echo -e "\n${BLUE}Secrets:${NC}"
    kubectl get secrets timescaledb-secret -n $NAMESPACE 2>/dev/null || echo "Secret not found"
}

# Function to show next steps
show_next_steps() {
    echo -e "\n${GREEN}🎉 Kafka Connect deployed successfully!${NC}"
    echo -e "\n${BLUE}📋 Next Steps:${NC}"
    echo -e "1. Update TimescaleDB credentials in secret:"
    echo -e "   ${YELLOW}kubectl edit secret timescaledb-secret -n $NAMESPACE${NC}"
    echo -e ""
    echo -e "2. Deploy TimescaleDB connector:"
    echo -e "   ${YELLOW}./scripts/connector-deploy.sh${NC}"
    echo -e ""
    echo -e "3. Check connector status:"
    echo -e "   ${YELLOW}./scripts/connector-status.sh${NC}"
    echo -e ""
    echo -e "4. Access Kafka Connect REST API:"
    echo -e "   ${YELLOW}kubectl port-forward service/kafka-connect $KAFKA_CONNECT_PORT:$KAFKA_CONNECT_PORT -n $NAMESPACE${NC}"
    echo -e "   ${YELLOW}curl http://localhost:$KAFKA_CONNECT_PORT/connectors${NC}"
    echo -e ""
    echo -e "5. Monitor logs:"
    echo -e "   ${YELLOW}kubectl logs -f deployment/kafka-connect -n $NAMESPACE${NC}"
}

# Main execution
main() {
    check_kubectl
    check_kafka_cluster
    deploy_kafka_connect
    wait_for_kafka_connect
    show_status
    show_next_steps
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --init-container)
            export USE_INIT_CONTAINER=true
            shift
            ;;
        --namespace|-n)
            NAMESPACE="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --init-container    Use init container to download connectors"
            echo "  --namespace, -n     Kubernetes namespace (default: default)"
            echo "  --help, -h          Show this help message"
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