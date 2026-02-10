"""Unit tests for storage manager."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from pathlib import Path
import pandas as pd
import tempfile
import shutil

from app.storage.manager import HierarchicalStorageManager
from app.config import StorageConfig


@pytest.fixture
def temp_storage_path():
    """Create temporary storage path."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def storage_config(temp_storage_path):
    """Create test storage configuration."""
    return StorageConfig(
        local_path=temp_storage_path,
        parquet_compression="lz4",
        partition_cols=[],
        max_rows_per_file=100
    )


@pytest.fixture
def storage_manager(storage_config):
    """Create storage manager instance."""
    return HierarchicalStorageManager(storage_config)


class TestHierarchicalStorageManager:
    """Test cases for HierarchicalStorageManager."""

    def test_initialization(self, storage_manager, storage_config, temp_storage_path):
        """Test storage manager initialization."""
        assert storage_manager.config == storage_config
        assert storage_manager.local_path == temp_storage_path
        assert storage_manager.compression == "lz4"
        assert storage_manager.max_rows == 100
        assert (temp_storage_path / "aggregated").exists()
        assert (temp_storage_path / "daily").exists()

    def test_extract_metadata(self, storage_manager):
        """Test metadata extraction."""
        data = {
            "sensor_name": "quad_ch1",
            "timestamp": "2024-01-01T12:30:45Z",
            "data": {
                "asset_id": "asset_001",
                "value": 42
            }
        }
        
        metadata = storage_manager.extract_metadata(data)
        
        assert metadata['asset_id'] == "asset_001"
        assert metadata['sensor_name'] == "quad_ch1"
        assert metadata['year'] == "2024"
        assert metadata['month'] == "01"
        assert metadata['day'] == "01"
        assert metadata['hour'] == "12"

    def test_extract_metadata_missing_fields(self, storage_manager):
        """Test metadata extraction with missing fields."""
        data = {"sensor_name": "test_sensor"}
        
        metadata = storage_manager.extract_metadata(data)
        
        assert metadata['sensor_name'] == "test_sensor"
        assert metadata['asset_id'] == "asset_test_sensor"
        assert 'year' in metadata
        assert 'month' in metadata

    def test_get_file_path(self, storage_manager, temp_storage_path):
        """Test file path generation."""
        metadata = {
            'asset_id': 'asset_001',
            'sensor_name': 'temp_sensor',
            'year': '2024',
            'month': '01',
            'day': '15',
            'hour': '14'
        }
        
        file_path = storage_manager.get_file_path(metadata)
        expected = temp_storage_path / "asset_001/2024/01/15/14/temp_sensor.parquet"
        
        assert file_path == expected

    def test_get_buffer_key(self, storage_manager):
        """Test buffer key generation."""
        metadata = {
            'asset_id': 'asset_001',
            'sensor_name': 'temp_sensor',
            'year': '2024',
            'month': '01',
            'day': '15',
            'hour': '14'
        }
        
        buffer_key = storage_manager.get_buffer_key(metadata)
        
        assert buffer_key == "asset_001/2024/01/15/14/temp_sensor"

    def test_add_to_buffer(self, storage_manager):
        """Test adding data to buffer."""
        data = {
            "sensor_name": "test_sensor",
            "timestamp": "2024-01-01T12:00:00Z",
            "topic": "sensor-data-test",
            "partition": 0,
            "offset": 100,
            "data": {
                "asset_id": "asset_001",
                "temperature": 25.5,
                "humidity": 60
            }
        }
        
        result = storage_manager.add_to_buffer(data)
        
        assert result is None  # Buffer not full yet
        assert len(storage_manager._file_buffers) == 1
        
        # Check buffer content
        buffer_key = list(storage_manager._file_buffers.keys())[0]
        buffer_data = storage_manager._file_buffers[buffer_key]
        assert len(buffer_data) == 1
        assert buffer_data[0]['temperature'] == 25.5
        assert buffer_data[0]['humidity'] == 60

    def test_flush_buffer(self, storage_manager, temp_storage_path):
        """Test buffer flushing."""
        # Add data to buffer
        data = {
            "sensor_name": "test_sensor",
            "timestamp": "2024-01-01T12:00:00Z",
            "topic": "sensor-data-test",
            "data": {
                "asset_id": "asset_001",
                "value": 42
            }
        }
        
        storage_manager.add_to_buffer(data)
        buffer_key = list(storage_manager._file_buffers.keys())[0]
        
        # Flush buffer
        file_path = storage_manager.flush_buffer(buffer_key)
        
        assert file_path is not None
        assert file_path.exists()
        assert buffer_key not in storage_manager._file_buffers
        
        # Verify file content
        df = pd.read_parquet(file_path)
        assert len(df) == 1
        assert df.iloc[0]['value'] == 42

    def test_flush_all_buffers(self, storage_manager):
        """Test flushing all buffers."""
        # Add data to multiple buffers
        for i in range(3):
            data = {
                "sensor_name": f"sensor_{i}",
                "timestamp": f"2024-01-01T{12+i}:00:00Z",
                "data": {"asset_id": f"asset_{i}", "value": i}
            }
            storage_manager.add_to_buffer(data)
        
        assert len(storage_manager._file_buffers) == 3
        
        # Flush all buffers
        flushed_files = storage_manager.flush_all_buffers()
        
        assert len(flushed_files) == 3
        assert len(storage_manager._file_buffers) == 0
        for file_path in flushed_files:
            assert file_path.exists()

    def test_get_local_files(self, storage_manager, temp_storage_path):
        """Test getting local files."""
        # Create test files
        test_file1 = temp_storage_path / "test1.parquet"
        test_file2 = temp_storage_path / "subdir/test2.parquet"
        test_file1.touch()
        test_file2.parent.mkdir(parents=True, exist_ok=True)
        test_file2.touch()
        
        files = storage_manager.get_local_files()
        
        assert len(files) == 2
        assert test_file1 in files
        assert test_file2 in files

    def test_get_storage_stats(self, storage_manager):
        """Test getting storage statistics."""
        # Add some data
        data = {
            "sensor_name": "test_sensor",
            "timestamp": "2024-01-01T12:00:00Z",
            "data": {"asset_id": "asset_001", "value": 42}
        }
        storage_manager.add_to_buffer(data)
        
        stats = storage_manager.get_storage_stats()
        
        assert stats['total_files'] == 0  # No flushed files yet
        assert stats['buffer_count'] == 1
        assert stats['buffered_records'] == 1
        assert 'assets' in stats

    def test_minute_buffer_aggregation(self, storage_manager):
        """Test minute-level aggregation buffer."""
        # Add numeric data
        for i in range(5):
            data = {
                "sensor_name": "temp_sensor",
                "timestamp": f"2024-01-01T12:00:{i*10:02d}Z",
                "data": {
                    "asset_id": "asset_001",
                    "temperature": 20 + i,
                    "pressure": 1000 + i
                }
            }
            storage_manager.add_to_buffer(data)
        
        # Check minute buffer
        assert len(storage_manager._minute_buffers) > 0
        minute_key = list(storage_manager._minute_buffers.keys())[0]
        minute_data = storage_manager._minute_buffers[minute_key]
        assert len(minute_data) == 5

    @patch('app.storage.manager.pd.DataFrame.to_parquet')
    def test_flush_minute_buffer(self, mock_to_parquet, storage_manager):
        """Test flushing minute aggregation buffer."""
        # Add data to minute buffer
        for i in range(3):
            data = {
                "sensor_name": "temp_sensor",
                "timestamp": f"2024-01-01T12:00:{i*20:02d}Z",
                "data": {
                    "asset_id": "asset_001",
                    "temperature": 20 + i
                }
            }
            storage_manager.add_to_buffer(data)
        
        # Get minute buffer key
        minute_key = list(storage_manager._minute_buffers.keys())[0]
        
        # Force flush minute buffer
        storage_manager._flush_minute_buffer(minute_key)
        
        # Verify buffer was cleared
        assert minute_key not in storage_manager._minute_buffers

    def test_buffer_auto_flush_on_max_rows(self, storage_manager):
        """Test automatic buffer flush when max rows reached."""
        # Add data up to max_rows
        for i in range(storage_manager.max_rows):
            data = {
                "sensor_name": "test_sensor",
                "timestamp": f"2024-01-01T12:00:{i:02d}Z",
                "data": {"asset_id": "asset_001", "value": i}
            }
            result = storage_manager.add_to_buffer(data)
            
            if i == storage_manager.max_rows - 1:
                # Should flush on last addition
                assert result is not None
                assert result.exists()

    @patch('app.storage.manager.datetime')
    def test_buffer_auto_flush_on_age(self, mock_datetime, storage_manager):
        """Test automatic buffer flush based on age."""
        # Set up mock time
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_datetime.utcnow.side_effect = [
            base_time,  # Initial buffer creation
            base_time,  # Metadata extraction
            base_time,  # Add to minute buffer
            base_time.replace(minute=6),  # Check age (6 minutes later)
            base_time.replace(minute=6),  # Flush time
        ]
        
        # Add data
        data = {
            "sensor_name": "test_sensor",
            "timestamp": "2024-01-01T12:00:00Z",
            "data": {"asset_id": "asset_001", "value": 42}
        }
        
        # First addition won't flush
        result1 = storage_manager.add_to_buffer(data)
        assert result1 is None
        
        # Second addition after 6 minutes should flush
        result2 = storage_manager.add_to_buffer(data)
        assert result2 is not None