@echo off
REM Batch script for starting development environment on Windows
REM For users who prefer .bat files over PowerShell

echo ================================
echo Sensor Data Storage Service
echo Development Environment Setup
echo ================================
echo.

REM Check if .env exists
if not exist ".env" (
    echo Creating .env file from template...
    copy .env.example .env
    echo Created .env file
    echo.
    echo Please edit .env with your configuration:
    echo   notepad .env
    echo.
    echo Then run this script again.
    pause
    exit /b 1
)

REM Check Docker
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker not found. Please install Docker Desktop.
    echo Download from: https://www.docker.com/products/docker-desktop/
    pause
    exit /b 1
)

echo Docker found
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not running. Please start Docker Desktop.
    pause
    exit /b 1
)
echo Docker is running
echo.

REM Create directories
echo Creating required directories...
if not exist "data\raw" mkdir "data\raw"
if not exist "data\aggregated" mkdir "data\aggregated"
if not exist "data\daily" mkdir "data\daily"
if not exist "logs" mkdir "logs"
echo Directories created
echo.

REM Start services
echo Starting services with Docker Compose...
echo This may take a few minutes on first run...
docker-compose --profile dev up -d

if %errorlevel% neq 0 (
    echo ERROR: Failed to start services
    echo Try running: docker-compose logs
    pause
    exit /b 1
)

echo.
echo ================================
echo Services Started!
echo ================================
echo.
echo Available endpoints:
echo   API:        http://localhost:8080
echo   Health:     http://localhost:8080/health
echo   Metrics:    http://localhost:8080/metrics
echo   API Docs:   http://localhost:8080/docs
echo   Kafka:      localhost:9092
echo.
echo Useful commands:
echo   View logs:     docker-compose logs -f sensor-storage-service
echo   Stop services: docker-compose down
echo   Test producer: python scripts/test-kafka-producer.py
echo.

set /p viewlogs="View logs now? (y/n): "
if /i "%viewlogs%"=="y" (
    echo.
    echo Showing logs (Press Ctrl+C to stop)...
    docker-compose logs -f sensor-storage-service
)

pause