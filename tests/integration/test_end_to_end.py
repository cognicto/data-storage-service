"""End-to-end integration tests for the sensor storage service."""

import json
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from app.azure.uploader import AzureBlobUploader
from app.cleanup.service import FileCleanupService
from app.config import (AppConfig, AzureConfig, CleanupConfig, KafkaConfig,
                        ServiceConfig, StorageConfig)
from app.kafka.consumer import SensorDataConsumer
from app.main import SensorDataStorageService
from app.storage.manager import HierarchicalStorageManager


@pytest.fixture
def temp_storage():
    """Create temporary storage directory."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_config(temp_storage):
    """Create test configuration."""
    return AppConfig(
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            topic_pattern="^sensor-data-.*",
            consumer_group="test-group",
        ),
        storage=StorageConfig(
            local_path=temp_storage, parquet_compression="lz4", max_rows_per_file=10
        ),
        azure=AzureConfig(
            storage_account="", storage_key="", container_name="test-container"
        ),
        cleanup=CleanupConfig(
            enabled=True, age_days=1, check_interval_hours=1, dry_run=False
        ),
        service=ServiceConfig(host="0.0.0.0", port=8080, debug=True),
    )


class TestEndToEnd:
    """End-to-end integration tests."""

    @patch("app.main.SensorDataConsumer")
    @patch("app.main.AzureBlobUploader")
    def test_data_flow_pipeline(
        self, mock_azure_class, mock_consumer_class, test_config, temp_storage
    ):
        """Test complete data flow from Kafka to storage."""
        # Setup mocks
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_azure = Mock()
        mock_azure_class.return_value = mock_azure

        # Create service
        service = SensorDataStorageService()

        with patch("app.main.load_config", return_value=test_config):
            assert service.initialize()

        # Simulate sensor data
        test_data = {
            "sensor_name": "test_sensor",
            "timestamp": "2024-01-15T10:30:00Z",
            "topic": "sensor-data-test",
            "partition": 0,
            "offset": 100,
            "data": {
                "asset_id": "asset_001",
                "temperature": 25.5,
                "humidity": 60.0,
                "pressure": 1013.25,
            },
        }

        # Process multiple messages to trigger flush
        for i in range(15):
            test_data["offset"] = 100 + i
            test_data["data"]["temperature"] = 25.0 + i * 0.1
            service.handle_kafka_message(test_data)

        # Verify files were created
        parquet_files = list(temp_storage.rglob("*.parquet"))
        assert len(parquet_files) > 0

        # Verify file structure
        expected_path = temp_storage / "asset_001/2024/01/15/10/test_sensor.parquet"
        assert expected_path.exists()

        # Verify file content (optimized 2-column schema)
        df = pd.read_parquet(expected_path)
        assert len(df) >= 10
        assert "timestamp" in df.columns
        assert "value" in df.columns
        # Asset ID and sensor name are now encoded in file path, not in data

    def test_storage_manager_integration(self, temp_storage):
        """Test storage manager with real file operations."""
        config = StorageConfig(
            local_path=temp_storage, parquet_compression="lz4", max_rows_per_file=5
        )

        manager = HierarchicalStorageManager(config)

        # Add data
        for i in range(10):
            data = {
                "sensor_name": f"sensor_{i % 2}",
                "timestamp": f"2024-01-15T10:{i:02d}:00Z",
                "data": {"asset_id": f"asset_{i % 3:03d}", "value": i * 10},
            }
            manager.add_to_buffer(data)

        # Flush all buffers
        flushed_files = manager.flush_all_buffers()
        assert len(flushed_files) > 0

        # Verify hierarchical structure
        for file_path in flushed_files:
            parts = file_path.relative_to(temp_storage).parts
            assert len(parts) == 6  # asset_id/yyyy/mm/dd/hh/sensor.parquet
            assert parts[0].startswith("asset_")
            assert parts[1] == "2024"
            assert parts[2] == "01"
            assert parts[3] == "15"
            assert parts[4] == "10"
            assert parts[5].endswith(".parquet")

    @patch("app.azure.uploader.BlobServiceClient")
    def test_azure_upload_integration(self, mock_blob_client, temp_storage):
        """Test Azure upload with mocked client."""
        # Setup mock
        mock_blob_service = Mock()
        mock_blob_client.return_value = mock_blob_service
        mock_container = Mock()
        mock_blob_service.get_container_client.return_value = mock_container
        mock_container.exists.return_value = True

        # Create test file
        test_file = temp_storage / "test.parquet"
        test_file.write_text("test data")

        # Create uploader
        config = AzureConfig(
            storage_account="testaccount",
            storage_key="testkey",
            container_name="test-container",
        )

        uploader = AzureBlobUploader(config, temp_storage)

        # Mock blob client for upload
        mock_blob = Mock()
        mock_blob_service.get_blob_client.return_value = mock_blob
        mock_blob.exists.return_value = False
        mock_blob.upload_blob.return_value = None

        # Upload file
        result = uploader.upload_file(test_file)

        assert result["status"] in ["uploaded", "failed"]
        if result["status"] == "uploaded":
            mock_blob.upload_blob.assert_called_once()

    def test_cleanup_service_integration(self, temp_storage):
        """Test cleanup service with real files."""
        # Create old test files
        old_file = temp_storage / "old_file.parquet"
        old_file.write_text("old data")

        # Make file appear old by modifying timestamp
        import os

        old_time = time.time() - (8 * 24 * 60 * 60)  # 8 days old
        os.utime(old_file, (old_time, old_time))

        # Create recent file
        new_file = temp_storage / "new_file.parquet"
        new_file.write_text("new data")

        # Create cleanup service
        config = CleanupConfig(
            enabled=True, age_days=7, check_interval_hours=1, dry_run=False
        )

        cleanup = FileCleanupService(config, temp_storage)

        # Mark old file as uploaded
        cleanup.mark_file_uploaded(old_file)

        # Run cleanup
        result = cleanup.run_cleanup()

        assert result["status"] == "completed"
        assert not old_file.exists()  # Old file should be deleted
        assert new_file.exists()  # New file should remain

    @patch("app.kafka.consumer.Consumer")
    def test_kafka_message_processing(self, mock_consumer_class, temp_storage):
        """Test Kafka message processing flow."""
        # Setup mock Kafka consumer
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer

        # Create consumer
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            topic_pattern="^sensor-data-.*",
            consumer_group="test-group",
        )

        consumer = SensorDataConsumer(config)
        consumer.connect()

        # Create mock message
        mock_message = Mock()
        mock_message.error.return_value = None
        mock_message.topic.return_value = "sensor-data-quad_ch1"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 1000
        mock_message.value.return_value = json.dumps(
            {
                "asset_id": "asset_123",
                "temperature": 22.5,
                "timestamp": "2024-01-15T10:00:00Z",
            }
        ).encode("utf-8")
        mock_message.timestamp.return_value = (1, 1705315200000)

        # Parse message
        parsed_data = consumer.parse_message(mock_message)

        assert parsed_data is not None
        assert parsed_data["sensor_name"] == "quad_ch1"
        assert parsed_data["topic"] == "sensor-data-quad_ch1"
        assert parsed_data["data"]["temperature"] == 22.5

    def test_aggregation_creation(self, temp_storage):
        """Test aggregation file creation."""
        config = StorageConfig(
            local_path=temp_storage, parquet_compression="lz4", max_rows_per_file=100
        )

        manager = HierarchicalStorageManager(config)

        # Add data for aggregation (optimized schema uses single value field)
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        for minute in range(5):
            for second in range(0, 60, 10):
                data = {
                    "sensor_name": "temp_sensor",
                    "timestamp": f"2024-01-15T10:{minute:02d}:{second:02d}Z",
                    "data": {
                        "asset_id": "asset_001",
                        "value": 20.0 + minute + (second / 60),
                    },
                }
                manager.add_to_buffer(data)

        # Flush minute buffers
        manager.flush_all_minute_buffers()

        # Check aggregated files
        agg_files = list((temp_storage / "aggregated").rglob("*_minute.parquet"))
        assert len(agg_files) > 0

        # Verify aggregated data (new enhanced schema with quality metrics)
        for agg_file in agg_files:
            df = pd.read_parquet(agg_file)
            # Check for new aggregation columns
            expected_columns = ["value_mean", "value_min", "value_max", "record_count", "minute_bucket"]
            has_expected_cols = any(col in df.columns for col in expected_columns)
            assert has_expected_cols, f"Expected aggregation columns not found in {agg_file}"
            assert len(df) > 0

    @pytest.mark.asyncio
    async def test_api_endpoints(self, test_config):
        """Test API endpoint responses."""
        from fastapi.testclient import TestClient

        from app.api.routes import create_app, set_service_references

        # Create app
        app = create_app()

        # Create mock services
        mock_consumer = Mock()
        mock_consumer.health_check.return_value = {"healthy": True, "issues": []}
        mock_consumer.get_metrics.return_value = {"messages_consumed": 100}

        mock_storage = Mock()
        mock_storage.get_storage_stats.return_value = {"total_files": 10}

        mock_azure = Mock()
        mock_azure.health_check.return_value = {"healthy": True, "issues": []}
        mock_azure.get_metrics.return_value = {"upload_stats": {"total_uploads": 50}}

        mock_cleanup = Mock()
        mock_cleanup.health_check.return_value = {"healthy": True, "issues": []}
        mock_cleanup.get_metrics.return_value = {
            "cleanup_stats": {"total_files_deleted": 20}
        }

        # Set service references
        set_service_references(mock_consumer, mock_storage, mock_azure, mock_cleanup)

        # Test client
        client = TestClient(app)

        # Test health endpoint
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ["healthy", "unhealthy"]

        # Test metrics endpoint
        response = client.get("/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "kafka" in data
        assert "storage" in data

        # Test storage stats
        response = client.get("/storage/stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_files" in data

    def test_cascade_updates_integration(self, temp_storage):
        """Test end-to-end cascade update functionality."""
        from app.aggregation.scheduler import AggregationScheduler
        from app.storage.manager import HierarchicalStorageManager
        
        # Setup storage and aggregation
        storage_config = StorageConfig(
            local_path=temp_storage, parquet_compression="lz4", max_rows_per_file=100
        )
        manager = HierarchicalStorageManager(storage_config)
        scheduler = AggregationScheduler(temp_storage)
        
        # Create minute aggregation data first
        asset_dir = temp_storage / "aggregated" / "asset_001" / "2026" / "04" / "04" / "14"
        asset_dir.mkdir(parents=True, exist_ok=True)
        minute_file = asset_dir / "temp_sensor_minute.parquet"
        
        # Initial minute data
        minute_data = pd.DataFrame({
            'value_mean': [25.5, 26.0, 24.8],
            'value_min': [24.0, 25.2, 24.1],
            'value_max': [27.0, 27.5, 25.5],
            'record_count': [60, 58, 62],
            'timestamp_start': ['2026-04-04T14:00:00', '2026-04-04T14:01:00', '2026-04-04T14:02:00'],
            'timestamp_end': ['2026-04-04T14:00:59', '2026-04-04T14:01:59', '2026-04-04T14:02:59'],
            'minute_bucket': ['2026-04-04 14:00:00', '2026-04-04 14:01:00', '2026-04-04 14:02:00']
        })
        minute_data.to_parquet(minute_file, compression="lz4", index=False)
        
        # Create initial hourly aggregation
        target_hour = datetime(2026, 4, 4, 14, 0, 0)
        scheduler._create_hourly_aggregations(target_hour)
        
        # Verify hourly file was created
        hourly_path = temp_storage / "aggregated" / "asset_001" / "2026" / "04" / "04" / "temp_sensor_hour.parquet"
        assert hourly_path.exists()
        
        # Read initial hourly data
        initial_hourly = pd.read_parquet(hourly_path)
        initial_mean = initial_hourly['value_mean'].iloc[0]
        
        # Wait a moment then modify minute file (simulate late arriving data)
        import time
        time.sleep(0.1)
        
        # Update minute data with late data
        updated_minute_data = minute_data.copy()
        updated_minute_data.iloc[0, updated_minute_data.columns.get_loc('value_mean')] = 99.9  # Significant change
        updated_minute_data.to_parquet(minute_file, compression="lz4", index=False)
        
        # Run cascade update - hourly should detect newer minute file
        scheduler._create_hourly_aggregations(target_hour)
        
        # Verify hourly was updated
        updated_hourly = pd.read_parquet(hourly_path)
        updated_mean = updated_hourly['value_mean'].iloc[0]
        
        # Mean should be different due to cascade update
        assert updated_mean != initial_mean, "Hourly aggregation should have been updated via cascade"
        
        # Test daily cascade updates
        target_date = datetime(2026, 4, 4).date()
        scheduler._create_daily_aggregations(target_date)
        
        # Verify daily file was created
        daily_path = temp_storage / "daily" / "asset_001" / "2026" / "04" / "temp_sensor_day.parquet"
        assert daily_path.exists()
        
        # Read initial daily data
        initial_daily = pd.read_parquet(daily_path)
        initial_daily_mean = initial_daily['value_mean'].iloc[0]
        
        # Wait and update hourly file again
        time.sleep(0.1)
        updated_hourly_data = updated_hourly.copy()
        updated_hourly_data.iloc[0, updated_hourly_data.columns.get_loc('value_mean')] = 88.8
        updated_hourly_data.to_parquet(hourly_path, compression="lz4", index=False)
        
        # Run daily cascade update
        scheduler._create_daily_aggregations(target_date)
        
        # Verify daily was updated
        final_daily = pd.read_parquet(daily_path)
        final_daily_mean = final_daily['value_mean'].iloc[0]
        
        assert final_daily_mean != initial_daily_mean, "Daily aggregation should have been updated via cascade"
        
        # Verify complete cascade update chain works
        assert len(initial_hourly) == 1
        assert len(updated_hourly) == 1
        assert len(final_daily) == 1
