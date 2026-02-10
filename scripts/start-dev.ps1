# PowerShell script for starting development environment on Windows
# Run as: .\scripts\start-dev.ps1

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Sensor Data Storage Service" -ForegroundColor Cyan
Write-Host "Development Environment Setup" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Check if running as administrator (recommended for Docker)
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if (-not $isAdmin) {
    Write-Host "WARNING: Not running as Administrator. Some operations may fail." -ForegroundColor Yellow
    Write-Host ""
}

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "Creating .env file from template..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host "✅ Created .env file" -ForegroundColor Green
    Write-Host ""
    Write-Host "⚠️  Please edit .env with your configuration:" -ForegroundColor Yellow
    Write-Host "   notepad .env" -ForegroundColor White
    Write-Host ""
    Write-Host "Then run this script again." -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

# Check Docker installation
Write-Host "Checking Docker installation..." -ForegroundColor Cyan
try {
    $dockerVersion = docker --version 2>$null
    if ($dockerVersion) {
        Write-Host "✅ Docker found: $dockerVersion" -ForegroundColor Green
    }
} catch {
    Write-Host "❌ Docker not found. Please install Docker Desktop for Windows." -ForegroundColor Red
    Write-Host "   Download from: https://www.docker.com/products/docker-desktop/" -ForegroundColor White
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if Docker is running
try {
    docker ps 2>$null | Out-Null
    Write-Host "✅ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not running. Please start Docker Desktop." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""

# Create necessary directories
Write-Host "Creating required directories..." -ForegroundColor Cyan
$directories = @(
    "data\raw",
    "data\aggregated", 
    "data\daily",
    "logs"
)

foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        Write-Host "✅ Created directory: $dir" -ForegroundColor Green
    } else {
        Write-Host "✓ Directory exists: $dir" -ForegroundColor Gray
    }
}

Write-Host ""

# Check for running containers
Write-Host "Checking for existing containers..." -ForegroundColor Cyan
$existingContainers = docker ps --format "table {{.Names}}" | Select-String "sensor-storage-service"
if ($existingContainers) {
    Write-Host "⚠️  Found existing containers. Stopping them..." -ForegroundColor Yellow
    docker-compose down
    Start-Sleep -Seconds 2
}

Write-Host ""

# Start services with docker-compose
Write-Host "Starting services with Docker Compose..." -ForegroundColor Cyan
Write-Host "This may take a few minutes on first run..." -ForegroundColor Gray
Write-Host ""

try {
    # Start in detached mode with dev profile
    docker-compose --profile dev up -d
    
    # Wait for services to start
    Write-Host ""
    Write-Host "Waiting for services to start..." -ForegroundColor Cyan
    
    $maxAttempts = 30
    $attempt = 0
    $serviceReady = $false
    
    while ($attempt -lt $maxAttempts -and -not $serviceReady) {
        Start-Sleep -Seconds 2
        $attempt++
        Write-Host "." -NoNewline
        
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                $serviceReady = $true
            }
        } catch {
            # Service not ready yet
        }
    }
    
    Write-Host ""
    Write-Host ""
    
    if ($serviceReady) {
        Write-Host "✅ Services started successfully!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Service may still be starting. Check logs for details." -ForegroundColor Yellow
    }
    
} catch {
    Write-Host "❌ Failed to start services: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Try running: docker-compose logs" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "Services Started!" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Available endpoints:" -ForegroundColor Cyan
Write-Host "  📡 API:        http://localhost:8080" -ForegroundColor White
Write-Host "  🏥 Health:     http://localhost:8080/health" -ForegroundColor White
Write-Host "  📊 Metrics:    http://localhost:8080/metrics" -ForegroundColor White
Write-Host "  📚 API Docs:   http://localhost:8080/docs" -ForegroundColor White
Write-Host "  🎯 Kafka:      localhost:9092" -ForegroundColor White
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Cyan
Write-Host "  View logs:     docker-compose logs -f sensor-storage-service" -ForegroundColor White
Write-Host "  Stop services: docker-compose down" -ForegroundColor White
Write-Host "  Test producer: python scripts/test-kafka-producer.py" -ForegroundColor White
Write-Host ""
Write-Host "Press Ctrl+C to keep services running in background" -ForegroundColor Gray
Write-Host ""

# Option to tail logs
$viewLogs = Read-Host "View logs now? (y/n)"
if ($viewLogs -eq 'y') {
    Write-Host ""
    Write-Host "Showing logs (Press Ctrl+C to stop)..." -ForegroundColor Cyan
    docker-compose logs -f sensor-storage-service
}