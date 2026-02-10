FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Create data directory with proper structure
RUN mkdir -p /data/aggregated /data/daily

# Create non-root user
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app /data
USER app

# Set Python path
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=45s --retries=3 \
    CMD curl -f http://localhost:8081/health/simple || exit 1

# Run the application
CMD ["python", "app/main.py"]