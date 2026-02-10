# Windows Setup Guide for Sensor Data Storage Service

This guide provides step-by-step instructions to set up and run the Sensor Data Storage Service on Windows systems.

## Prerequisites

### 1. Install Required Software

#### Python 3.11+
1. Download Python from [python.org](https://www.python.org/downloads/)
2. **IMPORTANT**: During installation, check "Add Python to PATH"
3. Verify installation:
```powershell
python --version
pip --version
```

#### Git
1. Download Git from [git-scm.com](https://git-scm.com/download/win)
2. Install with default settings
3. Verify:
```powershell
git --version
```

#### Docker Desktop (Recommended)
1. Download Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop/)
2. Install and restart Windows if prompted
3. Enable WSL2 when prompted during setup
4. Start Docker Desktop
5. Verify:
```powershell
docker --version
docker-compose --version
```

#### Azure Storage Explorer (Optional)
1. Download from [Microsoft Azure](https://azure.microsoft.com/en-us/features/storage-explorer/)
2. Install for managing Azure Blob Storage

## Setup Instructions

### Option 1: Docker Setup (Recommended)

#### Step 1: Clone Repository
```powershell
# Open PowerShell or Command Prompt
cd C:\Projects  # or your preferred directory
git clone https://github.com/your-org/sensor-data-storage-service.git
cd sensor-data-storage-service
```

#### Step 2: Configure Environment
```powershell
# Copy environment template
copy .env.example .env

# Edit .env file with Notepad or your preferred editor
notepad .env
```

Update these critical values in `.env`:
```ini
# Kafka Configuration (use host.docker.internal for local Kafka)
KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092

# Azure Configuration (optional)
AZURE_STORAGE_ACCOUNT=your_account
AZURE_STORAGE_KEY=your_key

# Local Storage (Windows path)
LOCAL_STORAGE_PATH=C:/data/sensor-storage
```

#### Step 3: Create Required Directories
```powershell
# Create data directories
mkdir -p C:\data\sensor-storage\raw
mkdir -p C:\data\sensor-storage\aggregated
mkdir -p C:\data\sensor-storage\daily
mkdir -p logs
```

#### Step 4: Start with Docker Compose
```powershell
# Start all services including Kafka
docker-compose --profile dev up -d

# Or just the service (if you have external Kafka)
docker-compose up -d

# Check logs
docker-compose logs -f sensor-storage-service
```

#### Step 5: Verify Installation
```powershell
# Check health
curl http://localhost:8080/health

# Or using PowerShell
Invoke-WebRequest -Uri http://localhost:8080/health | ConvertFrom-Json | Format-List
```

### Option 2: Native Python Setup

#### Step 1: Clone and Setup Virtual Environment
```powershell
# Clone repository
cd C:\Projects
git clone https://github.com/your-org/sensor-data-storage-service.git
cd sensor-data-storage-service

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\activate

# You should see (venv) in your prompt
```

#### Step 2: Install Dependencies
```powershell
# Upgrade pip
python -m pip install --upgrade pip

# Install production dependencies
pip install -r requirements.txt

# For development (includes testing tools)
pip install -r requirements-dev.txt
```

#### Step 3: Configure Environment
```powershell
# Copy environment template
copy .env.example .env

# Edit configuration
notepad .env
```

Windows-specific `.env` settings:
```ini
# Use Windows paths
LOCAL_STORAGE_PATH=C:\data\sensor-storage\raw
LOG_FILE_PATH=C:\logs\sensor-storage-service.log

# If using local Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

#### Step 4: Create Required Directories
```powershell
# Create directories
New-Item -ItemType Directory -Force -Path C:\data\sensor-storage\raw
New-Item -ItemType Directory -Force -Path C:\data\sensor-storage\aggregated
New-Item -ItemType Directory -Force -Path C:\data\sensor-storage\daily
New-Item -ItemType Directory -Force -Path C:\logs
```

#### Step 5: Run the Service
```powershell
# Make sure virtual environment is activated
.\venv\Scripts\activate

# Run the service
python -m app.main

# Or use the run script
python app/main.py
```

## Setting Up Kafka on Windows

### Option A: Using Docker (Easiest)
```powershell
# Start Kafka and Zookeeper with docker-compose
docker-compose --profile dev up -d kafka zookeeper

# Verify Kafka is running
docker ps
```

### Option B: Native Installation
1. Download Kafka from [kafka.apache.org](https://kafka.apache.org/downloads)
2. Extract to `C:\kafka`
3. Start Zookeeper:
```powershell
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```
4. Start Kafka (new terminal):
```powershell
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

## Testing the Installation

### 1. Run Health Check
```powershell
# Using curl (if installed)
curl http://localhost:8080/health

# Using PowerShell
$response = Invoke-WebRequest -Uri http://localhost:8080/health
$response.Content | ConvertFrom-Json | Format-List
```

### 2. Test Kafka Producer
```powershell
# Activate virtual environment
.\venv\Scripts\activate

# Run test producer
python scripts/test-kafka-producer.py
```

### 3. Check Metrics
```powershell
# View metrics
Invoke-WebRequest -Uri http://localhost:8080/metrics | Select-Object -ExpandProperty Content
```

### 4. Run Unit Tests
```powershell
# Activate virtual environment
.\venv\Scripts\activate

# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=app --cov-report=html
# Open coverage report
start htmlcov/index.html
```

## Windows-Specific Configurations

### File Paths
Always use forward slashes or escaped backslashes in configuration:
```ini
# Good
LOCAL_STORAGE_PATH=C:/data/sensor-storage
# Also good
LOCAL_STORAGE_PATH=C:\\data\\sensor-storage
# Bad (will cause errors)
LOCAL_STORAGE_PATH=C:\data\sensor-storage
```

### PowerShell Scripts
Create `start-dev.ps1` for Windows:
```powershell
# start-dev.ps1
Write-Host "Starting Sensor Data Storage Service..." -ForegroundColor Green

# Check if .env exists
if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "Created .env file. Please edit it and run again." -ForegroundColor Yellow
    exit 1
}

# Create directories
New-Item -ItemType Directory -Force -Path data/raw | Out-Null
New-Item -ItemType Directory -Force -Path logs | Out-Null

# Start with docker-compose
docker-compose --profile dev up -d

Write-Host "`nServices started!" -ForegroundColor Green
Write-Host "API: http://localhost:8080"
Write-Host "Health: http://localhost:8080/health"
Write-Host "Logs: docker-compose logs -f sensor-storage-service"
```

### Windows Service Installation (Optional)
To run as a Windows service:

1. Install NSSM (Non-Sucking Service Manager):
```powershell
choco install nssm
```

2. Create service:
```powershell
nssm install SensorStorageService "C:\Projects\sensor-data-storage-service\venv\Scripts\python.exe" "C:\Projects\sensor-data-storage-service\app\main.py"
nssm set SensorStorageService AppDirectory "C:\Projects\sensor-data-storage-service"
nssm set SensorStorageService AppEnvironmentExtra "PYTHONPATH=C:\Projects\sensor-data-storage-service"
```

3. Start service:
```powershell
nssm start SensorStorageService
```

## Troubleshooting

### Common Issues

#### 1. Port Already in Use
```powershell
# Find process using port 8080
netstat -ano | findstr :8080
# Kill process (replace PID with actual process ID)
taskkill /PID <PID> /F
```

#### 2. Docker Not Starting
- Ensure WSL2 is enabled
- Restart Docker Desktop
- Run as Administrator

#### 3. Python Module Not Found
```powershell
# Ensure virtual environment is activated
.\venv\Scripts\activate
# Reinstall dependencies
pip install -r requirements.txt
```

#### 4. Kafka Connection Failed
- Check Windows Firewall settings
- Allow ports 9092 (Kafka) and 2181 (Zookeeper)
- Use `localhost` instead of `127.0.0.1`

#### 5. File Permission Errors
- Run PowerShell/CMD as Administrator
- Check folder permissions for data directories

### Windows Firewall Configuration
Add firewall rules for the service:
```powershell
# Run as Administrator
New-NetFirewallRule -DisplayName "Sensor Storage API" -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow
New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -Protocol TCP -LocalPort 9092 -Action Allow
New-NetFirewallRule -DisplayName "Zookeeper" -Direction Inbound -Protocol TCP -LocalPort 2181 -Action Allow
```

## Development Tools for Windows

### Recommended IDEs
- **Visual Studio Code**: Free, lightweight, excellent Python support
- **PyCharm**: Full-featured Python IDE (Community edition is free)
- **Visual Studio 2022**: Full IDE with Python workload

### VS Code Setup
1. Install VS Code
2. Install extensions:
   - Python
   - Docker
   - Azure Storage
   - GitLens
3. Open project: `code C:\Projects\sensor-data-storage-service`

### Monitoring Tools
- **Azure Storage Explorer**: View uploaded files
- **Offset Explorer** (formerly Kafka Tool): Monitor Kafka topics
- **Postman**: Test API endpoints

## Performance Optimization for Windows

### Memory Settings
Edit `.env` for Windows memory constraints:
```ini
# Reduce memory usage on Windows
MAX_ROWS_PER_FILE=50000
BUFFER_FLUSH_INTERVAL_SECONDS=180
AZURE_MAX_WORKERS=2
```

### Docker Desktop Settings
1. Open Docker Desktop Settings
2. Go to Resources
3. Adjust:
   - CPUs: 2-4
   - Memory: 4-8 GB
   - Disk: 20+ GB

## Production Deployment on Windows Server

For Windows Server deployment:

1. Use IIS with FastAPI:
   - Install IIS with CGI
   - Configure reverse proxy to FastAPI

2. Or use Windows Container:
```powershell
docker build -t sensor-storage-service .
docker run -d --restart=always --name sensor-storage -p 8080:8080 sensor-storage-service
```

## Getting Help

### Logs Location
- Docker logs: `docker-compose logs sensor-storage-service`
- Native Python: `C:\logs\sensor-storage-service.log`
- Windows Event Viewer (if running as service)

### Debug Mode
Set in `.env`:
```ini
SERVICE_DEBUG=true
LOG_LEVEL=DEBUG
```

### Support Resources
- GitHub Issues: Report Windows-specific issues
- Documentation: `/docs` folder
- API Docs: http://localhost:8080/docs (when running)

## Next Steps

1. ✅ Service is running
2. ✅ Health check passes
3. 📝 Configure Azure credentials (optional)
4. 🚀 Start sending data to Kafka topics
5. 📊 Monitor via API endpoints
6. 🔧 Customize configuration as needed

---

**Note**: This guide assumes Windows 10/11 or Windows Server 2019+. For older versions, some commands may differ.