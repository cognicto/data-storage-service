#!/bin/bash
# Kafka Connect Connector Status and Management Script

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

# Function to create port-forward
create_port_forward() {
    # Kill any existing port-forwards
    pkill -f "kubectl port-forward.*kafka-connect" 2>/dev/null || true
    
    # Create new port-forward
    kubectl port-forward service/$KAFKA_CONNECT_SERVICE $KAFKA_CONNECT_PORT:$KAFKA_CONNECT_PORT -n $NAMESPACE &
    PF_PID=$!
    
    # Wait for port-forward to be established
    sleep 2
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

# Function to show Kafka Connect cluster status
show_cluster_status() {
    echo -e "${BLUE}📊 Kafka Connect Cluster Status${NC}"
    echo "================================"
    
    CLUSTER_INFO=$(curl -s http://localhost:$KAFKA_CONNECT_PORT/)
    if [ $? -eq 0 ]; then
        echo "$CLUSTER_INFO" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f'Version: {data.get(\"version\", \"unknown\")}')
    print(f'Commit: {data.get(\"commit\", \"unknown\")}')
    print(f'Kafka Cluster ID: {data.get(\"kafka_cluster_id\", \"unknown\")}')
except:
    print('Failed to parse cluster info')
"
    else
        echo -e "${RED}❌ Failed to connect to Kafka Connect${NC}"
        return 1
    fi
}

# Function to list all connectors
list_connectors() {
    echo -e "\n${BLUE}🔌 Active Connectors${NC}"
    echo "==================="
    
    CONNECTORS=$(curl -s http://localhost:$KAFKA_CONNECT_PORT/connectors)
    if [ $? -eq 0 ]; then
        CONNECTOR_COUNT=$(echo "$CONNECTORS" | python3 -c "import sys, json; print(len(json.load(sys.stdin)))")
        echo -e "Total connectors: ${GREEN}$CONNECTOR_COUNT${NC}"
        
        if [ "$CONNECTOR_COUNT" -gt 0 ]; then
            echo "$CONNECTORS" | python3 -c "
import sys, json
connectors = json.load(sys.stdin)
for i, connector in enumerate(connectors, 1):
    print(f'{i}. {connector}')
"
        else
            echo -e "${YELLOW}No connectors found${NC}"
        fi
    else
        echo -e "${RED}❌ Failed to retrieve connector list${NC}"
    fi
}

# Function to show specific connector status
show_connector_status() {
    local connector_name="${1:-$CONNECTOR_NAME}"
    
    echo -e "\n${BLUE}📋 Connector: $connector_name${NC}"
    echo "================================"
    
    STATUS=$(curl -s http://localhost:$KAFKA_CONNECT_PORT/connectors/$connector_name/status)
    if [ $? -eq 0 ] && ! echo "$STATUS" | grep -q "error"; then
        echo "$STATUS" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    
    # Connector status
    connector = data.get('connector', {})
    state = connector.get('state', 'UNKNOWN')
    worker_id = connector.get('worker_id', 'unknown')
    
    print(f'State: {state}')
    print(f'Worker: {worker_id}')
    
    # Tasks status
    tasks = data.get('tasks', [])
    print(f'\\nTasks: {len(tasks)}')
    for task in tasks:
        task_id = task.get('id', 'unknown')
        task_state = task.get('state', 'unknown')
        task_worker = task.get('worker_id', 'unknown')
        print(f'  Task {task_id}: {task_state} on {task_worker}')
        
        if 'trace' in task:
            print(f'    Error: {task.get(\"trace\", \"No details\")[:100]}...')
            
except Exception as e:
    print(f'Error parsing status: {e}')
"
    else
        echo -e "${RED}❌ Connector '$connector_name' not found or error occurred${NC}"
        if echo "$STATUS" | grep -q "error"; then
            echo "$STATUS" | python3 -m json.tool
        fi
    fi
}

# Function to show connector configuration
show_connector_config() {
    local connector_name="${1:-$CONNECTOR_NAME}"
    
    echo -e "\n${BLUE}⚙️  Connector Configuration: $connector_name${NC}"
    echo "========================================="
    
    CONFIG=$(curl -s http://localhost:$KAFKA_CONNECT_PORT/connectors/$connector_name/config)
    if [ $? -eq 0 ] && ! echo "$CONFIG" | grep -q "error"; then
        echo "$CONFIG" | python3 -m json.tool
    else
        echo -e "${RED}❌ Failed to retrieve configuration for '$connector_name'${NC}"
    fi
}

# Function to show connector tasks
show_connector_tasks() {
    local connector_name="${1:-$CONNECTOR_NAME}"
    
    echo -e "\n${BLUE}📝 Connector Tasks: $connector_name${NC}"
    echo "==============================="
    
    TASKS=$(curl -s http://localhost:$KAFKA_CONNECT_PORT/connectors/$connector_name/tasks)
    if [ $? -eq 0 ] && ! echo "$TASKS" | grep -q "error"; then
        echo "$TASKS" | python3 -c "
import sys, json
try:
    tasks = json.load(sys.stdin)
    for task in tasks:
        task_id = task.get('id', {})
        config = task.get('config', {})
        print(f'Task {task_id}:')
        for key, value in config.items():
            print(f'  {key}: {value}')
        print()
except Exception as e:
    print(f'Error parsing tasks: {e}')
"
    else
        echo -e "${RED}❌ Failed to retrieve tasks for '$connector_name'${NC}"
    fi
}

# Function to restart connector
restart_connector() {
    local connector_name="${1:-$CONNECTOR_NAME}"
    
    echo -e "${BLUE}🔄 Restarting connector: $connector_name${NC}"
    
    RESTART_RESPONSE=$(curl -s -X POST http://localhost:$KAFKA_CONNECT_PORT/connectors/$connector_name/restart)
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Restart request sent${NC}"
        sleep 3
        show_connector_status "$connector_name"
    else
        echo -e "${RED}❌ Failed to restart connector${NC}"
    fi
}

# Function to pause connector
pause_connector() {
    local connector_name="${1:-$CONNECTOR_NAME}"
    
    echo -e "${BLUE}⏸️  Pausing connector: $connector_name${NC}"
    
    curl -s -X PUT http://localhost:$KAFKA_CONNECT_PORT/connectors/$connector_name/pause
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Connector paused${NC}"
    else
        echo -e "${RED}❌ Failed to pause connector${NC}"
    fi
}

# Function to resume connector
resume_connector() {
    local connector_name="${1:-$CONNECTOR_NAME}"
    
    echo -e "${BLUE}▶️  Resuming connector: $connector_name${NC}"
    
    curl -s -X PUT http://localhost:$KAFKA_CONNECT_PORT/connectors/$connector_name/resume
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Connector resumed${NC}"
    else
        echo -e "${RED}❌ Failed to resume connector${NC}"
    fi
}

# Function to delete connector
delete_connector() {
    local connector_name="${1:-$CONNECTOR_NAME}"
    
    echo -e "${RED}🗑️  Deleting connector: $connector_name${NC}"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        curl -s -X DELETE http://localhost:$KAFKA_CONNECT_PORT/connectors/$connector_name
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ Connector deleted${NC}"
        else
            echo -e "${RED}❌ Failed to delete connector${NC}"
        fi
    else
        echo -e "${YELLOW}Deletion cancelled${NC}"
    fi
}

# Function to show available plugins
show_plugins() {
    echo -e "\n${BLUE}🔌 Available Connector Plugins${NC}"
    echo "==============================="
    
    PLUGINS=$(curl -s http://localhost:$KAFKA_CONNECT_PORT/connector-plugins)
    if [ $? -eq 0 ]; then
        echo "$PLUGINS" | python3 -c "
import sys, json
try:
    plugins = json.load(sys.stdin)
    for plugin in plugins:
        class_name = plugin.get('class', 'unknown')
        plugin_type = plugin.get('type', 'unknown')
        version = plugin.get('version', 'unknown')
        print(f'{class_name}')
        print(f'  Type: {plugin_type}')
        print(f'  Version: {version}')
        print()
except Exception as e:
    print(f'Error parsing plugins: {e}')
"
    else
        echo -e "${RED}❌ Failed to retrieve plugins${NC}"
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [COMMAND] [CONNECTOR_NAME]"
    echo ""
    echo "Commands:"
    echo "  status [name]     Show connector status (default: $CONNECTOR_NAME)"
    echo "  config [name]     Show connector configuration"
    echo "  tasks [name]      Show connector tasks"
    echo "  list              List all connectors"
    echo "  restart [name]    Restart connector"
    echo "  pause [name]      Pause connector"
    echo "  resume [name]     Resume connector"
    echo "  delete [name]     Delete connector"
    echo "  plugins           Show available connector plugins"
    echo "  cluster           Show cluster status"
    echo "  help              Show this help message"
    echo ""
    echo "If no command is specified, shows comprehensive status information."
}

# Main execution
main() {
    local command="${1:-status}"
    local connector_name="${2:-$CONNECTOR_NAME}"
    
    case $command in
        status)
            create_port_forward
            show_cluster_status
            list_connectors
            show_connector_status "$connector_name"
            ;;
        config)
            create_port_forward
            show_connector_config "$connector_name"
            ;;
        tasks)
            create_port_forward
            show_connector_tasks "$connector_name"
            ;;
        list)
            create_port_forward
            show_cluster_status
            list_connectors
            ;;
        restart)
            create_port_forward
            restart_connector "$connector_name"
            ;;
        pause)
            create_port_forward
            pause_connector "$connector_name"
            ;;
        resume)
            create_port_forward
            resume_connector "$connector_name"
            ;;
        delete)
            create_port_forward
            delete_connector "$connector_name"
            ;;
        plugins)
            create_port_forward
            show_plugins
            ;;
        cluster)
            create_port_forward
            show_cluster_status
            ;;
        help|--help|-h)
            show_usage
            cleanup_and_exit 0
            ;;
        *)
            echo -e "${RED}Unknown command: $command${NC}"
            show_usage
            cleanup_and_exit 1
            ;;
    esac
    
    cleanup_and_exit 0
}

# Run main function with all arguments
main "$@"