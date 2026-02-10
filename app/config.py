"""
Configuration management for the sensor data storage service.
"""

import os
from typing import List, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class KafkaConfig:
    """Kafka consumer configuration."""
    bootstrap_servers: str
    topic_pattern: str
    consumer_group: str
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = False
    session_timeout_ms: int = 30000
    max_poll_interval_ms: int = 300000
    max_poll_records: int = 500


@dataclass
class AzureConfig:
    """Azure Blob Storage configuration."""
    blob_endpoint: str  # Full blob endpoint URL
    sas_token: str  # SAS token (with or without leading ?)
    container_name: str
    storage_account: str = ""  # Will be extracted from blob_endpoint
    storage_key: str = ""  # Kept for backward compatibility
    max_retries: int = 3
    retry_delay: int = 1
    max_workers: int = 4
    chunk_size: int = 1024 * 1024  # 1MB chunks


@dataclass
class StorageConfig:
    """Local storage configuration."""
    local_path: Path
    parquet_compression: str = "lz4"
    partition_cols: List[str] = None
    max_rows_per_file: int = 100000


@dataclass
class CleanupConfig:
    """File cleanup configuration."""
    enabled: bool
    age_days: int
    check_interval_hours: int = 6
    dry_run: bool = False


@dataclass
class ServiceConfig:
    """API and service configuration."""
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False
    log_level: str = "INFO"
    workers: int = 1


@dataclass
class AppConfig:
    """Complete application configuration."""
    kafka: KafkaConfig
    azure: AzureConfig
    storage: StorageConfig
    cleanup: CleanupConfig
    service: ServiceConfig


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    
    # Kafka configuration
    kafka_config = KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic_pattern=os.getenv("KAFKA_TOPIC_PATTERN", "^sensor-data-quad.*"),
        consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "sensor-storage-service"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
        enable_auto_commit=os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true",
        session_timeout_ms=int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000")),
        max_poll_interval_ms=int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000")),
        max_poll_records=int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500"))
    )
    
    # Azure configuration
    azure_config = AzureConfig(
        blob_endpoint=os.getenv("AZURE_BLOB_ENDPOINT", ""),
        sas_token=os.getenv("AZURE_SAS_TOKEN", ""),
        container_name=os.getenv("AZURE_CONTAINER_NAME", "sensor-data-cold-storage"),
        storage_account=os.getenv("AZURE_STORAGE_ACCOUNT", ""),  # For backward compatibility
        storage_key=os.getenv("AZURE_STORAGE_KEY", ""),  # For backward compatibility
        max_retries=int(os.getenv("AZURE_MAX_RETRIES", "3")),
        retry_delay=int(os.getenv("AZURE_RETRY_DELAY", "1")),
        max_workers=int(os.getenv("AZURE_MAX_WORKERS", "4")),
        chunk_size=int(os.getenv("AZURE_CHUNK_SIZE", str(1024 * 1024)))
    )
    
    # Storage configuration
    # Handle Windows paths correctly
    storage_path_str = os.getenv("LOCAL_STORAGE_PATH", "/data/raw")
    # Convert Windows path format if needed
    if storage_path_str.startswith("C:") or storage_path_str.startswith("c:"):
        storage_path = Path(storage_path_str.replace("/", "\\"))
    else:
        storage_path = Path(storage_path_str)
    
    storage_config = StorageConfig(
        local_path=storage_path,
        parquet_compression=os.getenv("PARQUET_COMPRESSION", "lz4"),
        partition_cols=os.getenv("PARQUET_PARTITION_COLS", "").split(",") if os.getenv("PARQUET_PARTITION_COLS") else [],
        max_rows_per_file=int(os.getenv("MAX_ROWS_PER_FILE", "100000"))
    )
    
    # Cleanup configuration
    cleanup_config = CleanupConfig(
        enabled=os.getenv("CLEANUP_ENABLED", "true").lower() == "true",
        age_days=int(os.getenv("CLEANUP_AGE_DAYS", "7")),
        check_interval_hours=int(os.getenv("CLEANUP_INTERVAL_HOURS", "6")),
        dry_run=os.getenv("CLEANUP_DRY_RUN", "false").lower() == "true"
    )
    
    # Service configuration
    service_config = ServiceConfig(
        host=os.getenv("SERVICE_HOST", "0.0.0.0"),
        port=int(os.getenv("SERVICE_PORT", "8080")),
        debug=os.getenv("SERVICE_DEBUG", "false").lower() == "true",
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        workers=int(os.getenv("SERVICE_WORKERS", "1"))
    )
    
    return AppConfig(
        kafka=kafka_config,
        azure=azure_config,
        storage=storage_config,
        cleanup=cleanup_config,
        service=service_config
    )


def validate_config(config: AppConfig) -> bool:
    """Validate the configuration."""
    errors = []
    
    # Validate Kafka config
    if not config.kafka.bootstrap_servers:
        errors.append("KAFKA_BOOTSTRAP_SERVERS is required")
    
    if not config.kafka.topic_pattern:
        errors.append("KAFKA_TOPIC_PATTERN is required")
    
    # Validate Azure config (only if not disabled)
    if config.azure.storage_account and not config.azure.storage_key:
        errors.append("AZURE_STORAGE_KEY is required when AZURE_STORAGE_ACCOUNT is set")
    
    # Validate storage path
    if not config.storage.local_path:
        errors.append("LOCAL_STORAGE_PATH is required")
    
    if errors:
        for error in errors:
            print(f"Configuration error: {error}")
        return False
    
    return True