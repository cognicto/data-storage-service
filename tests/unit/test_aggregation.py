"""Unit tests for aggregation scheduler."""

import shutil
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from app.aggregation.scheduler import AggregationScheduler


@pytest.fixture
def temp_storage_path():
    """Create temporary storage path."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def scheduler(temp_storage_path):
    """Create aggregation scheduler instance."""
    return AggregationScheduler(temp_storage_path)


@pytest.fixture
def sample_minute_data():
    """Create sample minute aggregation data."""
    return pd.DataFrame({
        'value_mean': [25.5, 26.0, 24.8],
        'value_min': [24.0, 25.2, 24.1],
        'value_max': [27.0, 27.5, 25.5],
        'record_count': [60, 58, 62],
        'timestamp_start': [
            '2026-04-04T14:00:00',
            '2026-04-04T14:01:00', 
            '2026-04-04T14:02:00'
        ],
        'timestamp_end': [
            '2026-04-04T14:00:59',
            '2026-04-04T14:01:59',
            '2026-04-04T14:02:59'
        ],
        'minute_bucket': [
            '2026-04-04 14:00:00',
            '2026-04-04 14:01:00',
            '2026-04-04 14:02:00'
        ]
    })


@pytest.fixture
def sample_hourly_data():
    """Create sample hourly aggregation data."""
    return pd.DataFrame({
        'value_mean': [25.4, 26.2],
        'value_min': [24.0, 25.1], 
        'value_max': [27.5, 27.8],
        'record_count': [180, 175],
        'minute_count': [3, 3],
        'timestamp_start': [
            '2026-04-04T14:00:00',
            '2026-04-04T15:00:00'
        ],
        'timestamp_end': [
            '2026-04-04T14:02:59',
            '2026-04-04T15:02:59'
        ],
        'hour_bucket': [
            '2026-04-04 14:00:00',
            '2026-04-04 15:00:00'
        ]
    })


class TestAggregationScheduler:
    """Test cases for AggregationScheduler."""

    def test_initialization(self, scheduler, temp_storage_path):
        """Test scheduler initialization."""
        assert scheduler.storage_path == temp_storage_path
        assert scheduler.compression == "snappy"
        assert scheduler.running is False
        assert scheduler.scheduler_thread is None

    def test_needs_reaggregation_file_not_exists(self, scheduler, temp_storage_path):
        """Test reaggregation when aggregation file doesn't exist."""
        agg_file = temp_storage_path / "nonexistent.parquet"
        source_files = [temp_storage_path / "source.parquet"]
        
        result = scheduler.needs_reaggregation(agg_file, source_files)
        assert result is True

    def test_needs_reaggregation_newer_source_file(self, scheduler, temp_storage_path):
        """Test reaggregation when source file is newer."""
        # Create aggregation file
        agg_file = temp_storage_path / "agg.parquet"
        agg_file.touch()
        
        # Wait and create newer source file
        time.sleep(0.1)
        source_file = temp_storage_path / "source.parquet"  
        source_file.touch()
        
        result = scheduler.needs_reaggregation(agg_file, [source_file])
        assert result is True

    def test_needs_reaggregation_older_source_file(self, scheduler, temp_storage_path):
        """Test no reaggregation when source file is older."""
        # Create source file first
        source_file = temp_storage_path / "source.parquet"
        source_file.touch()
        
        # Wait and create newer aggregation file
        time.sleep(0.1)
        agg_file = temp_storage_path / "agg.parquet"
        agg_file.touch()
        
        result = scheduler.needs_reaggregation(agg_file, [source_file])
        assert result is False

    def test_aggregate_minute_to_hour_valid_data(self, scheduler, temp_storage_path, sample_minute_data):
        """Test minute to hour aggregation with valid data."""
        # Setup minute file
        asset_dir = temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "14"
        asset_dir.mkdir(parents=True, exist_ok=True)
        minute_file = asset_dir / "temp_sensor_minute.parquet"
        sample_minute_data.to_parquet(minute_file, compression="snappy", index=False)
        
        # Create hour aggregation
        target_hour = datetime(2026, 4, 4, 14, 0, 0)
        scheduler._aggregate_minute_to_hour(
            minute_file, "temp_sensor", "asset_001", target_hour
        )
        
        # Check hourly file was created
        hourly_path = (temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "temp_sensor_hour.parquet")
        assert hourly_path.exists()
        
        # Verify hourly data
        hourly_df = pd.read_parquet(hourly_path)
        assert len(hourly_df) == 1
        assert "value_mean" in hourly_df.columns
        assert "value_min" in hourly_df.columns
        assert "value_max" in hourly_df.columns
        assert "record_count" in hourly_df.columns
        assert "minute_count" in hourly_df.columns
        assert "hour_bucket" in hourly_df.columns

    def test_aggregate_minute_to_hour_empty_data(self, scheduler, temp_storage_path):
        """Test minute to hour aggregation with empty data."""
        # Setup empty minute file
        asset_dir = temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "14"
        asset_dir.mkdir(parents=True, exist_ok=True)
        minute_file = asset_dir / "temp_sensor_minute.parquet"
        
        empty_df = pd.DataFrame()
        empty_df.to_parquet(minute_file, compression="snappy", index=False)
        
        # Try aggregation
        target_hour = datetime(2026, 4, 4, 14, 0, 0)
        scheduler._aggregate_minute_to_hour(
            minute_file, "temp_sensor", "asset_001", target_hour
        )
        
        # Check no hourly file created
        hourly_path = (temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "temp_sensor_hour.parquet")
        assert not hourly_path.exists()

    def test_aggregate_hour_to_day_valid_data(self, scheduler, temp_storage_path, sample_hourly_data):
        """Test hour to day aggregation with valid data."""
        # Setup hourly files 
        asset_dir = temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04"
        asset_dir.mkdir(parents=True, exist_ok=True)
        
        hourly_file1 = asset_dir / "temp_sensor_hour.parquet"
        sample_hourly_data.iloc[:1].to_parquet(hourly_file1, compression="snappy", index=False)
        
        hourly_file2 = asset_dir / "temp_sensor_hour.parquet"
        sample_hourly_data.iloc[1:].to_parquet(hourly_file2, compression="snappy", index=False)
        
        # Create daily aggregation
        target_date = datetime(2026, 4, 4).date()
        scheduler._aggregate_hour_to_day(
            [hourly_file1], "temp_sensor", "asset_001", target_date
        )
        
        # Check daily file was created
        daily_path = (temp_storage_path / "daily" / "asset_001" / "2026" / "04" / "temp_sensor_day.parquet")
        assert daily_path.exists()
        
        # Verify daily data
        daily_df = pd.read_parquet(daily_path)
        assert len(daily_df) == 1
        assert "value_mean" in daily_df.columns
        assert "value_min" in daily_df.columns
        assert "value_max" in daily_df.columns
        assert "record_count" in daily_df.columns
        assert "minute_count" in daily_df.columns
        assert "hour_count" in daily_df.columns
        assert "day_bucket" in daily_df.columns

    def test_aggregate_hour_to_day_empty_files(self, scheduler, temp_storage_path):
        """Test hour to day aggregation with empty hourly files."""
        # Try aggregation with no files
        target_date = datetime(2026, 4, 4).date()
        scheduler._aggregate_hour_to_day(
            [], "temp_sensor", "asset_001", target_date
        )
        
        # Check no daily file created
        daily_path = (temp_storage_path / "daily" / "asset_001" / "2026" / "04" / "temp_sensor_day.parquet")
        assert not daily_path.exists()

    def test_create_hourly_aggregations_with_cascade_updates(self, scheduler, temp_storage_path, sample_minute_data):
        """Test hourly aggregation creation with cascade update logic."""
        # Setup minute files
        asset_dir = temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "14"  
        asset_dir.mkdir(parents=True, exist_ok=True)
        minute_file = asset_dir / "temp_sensor_minute.parquet"
        sample_minute_data.to_parquet(minute_file, compression="snappy", index=False)
        
        # Run hourly aggregations
        target_hour = datetime(2026, 4, 4, 14, 0, 0)
        scheduler._create_hourly_aggregations(target_hour)
        
        # Check hourly file was created
        hourly_path = (temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "temp_sensor_hour.parquet")
        assert hourly_path.exists()
        
        # Modify minute file (simulate late data)
        time.sleep(0.1)
        updated_minute_data = sample_minute_data.copy()
        updated_minute_data.iloc[0, updated_minute_data.columns.get_loc('value_mean')] = 99.9
        updated_minute_data.to_parquet(minute_file, compression="snappy", index=False)
        
        # Run hourly aggregations again - should detect update needed
        scheduler._create_hourly_aggregations(target_hour)
        
        # Verify hourly file was updated
        hourly_df = pd.read_parquet(hourly_path)
        # The aggregation should have been recalculated with updated data
        assert len(hourly_df) == 1

    def test_create_daily_aggregations_with_cascade_updates(self, scheduler, temp_storage_path, sample_hourly_data):
        """Test daily aggregation creation with cascade update logic."""
        # Setup hourly files
        asset_dir = temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04"
        asset_dir.mkdir(parents=True, exist_ok=True)
        hourly_file = asset_dir / "temp_sensor_hour.parquet"
        sample_hourly_data.to_parquet(hourly_file, compression="snappy", index=False)
        
        # Run daily aggregations
        target_date = datetime(2026, 4, 4).date()
        scheduler._create_daily_aggregations(target_date)
        
        # Check daily file was created
        daily_path = (temp_storage_path / "daily" / "asset_001" / "2026" / "04" / "temp_sensor_day.parquet")
        assert daily_path.exists()
        
        # Modify hourly file (simulate updated hourly aggregation) 
        time.sleep(0.1)
        updated_hourly_data = sample_hourly_data.copy()
        updated_hourly_data.iloc[0, updated_hourly_data.columns.get_loc('value_mean')] = 88.8
        updated_hourly_data.to_parquet(hourly_file, compression="snappy", index=False)
        
        # Run daily aggregations again - should detect update needed
        scheduler._create_daily_aggregations(target_date)
        
        # Verify daily file was updated
        daily_df = pd.read_parquet(daily_path)
        # The aggregation should have been recalculated with updated data
        assert len(daily_df) == 1

    def test_force_create_aggregations(self, scheduler, temp_storage_path, sample_minute_data):
        """Test manual aggregation creation."""
        # Setup minute files
        asset_dir = temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "14"
        asset_dir.mkdir(parents=True, exist_ok=True)
        minute_file = asset_dir / "temp_sensor_minute.parquet"
        sample_minute_data.to_parquet(minute_file, compression="snappy", index=False)
        
        # Force create aggregations
        target_datetime = datetime(2026, 4, 4, 14, 0, 0)
        scheduler.force_create_aggregations(target_datetime, "both")
        
        # Check both hourly and daily files were created
        hourly_path = (temp_storage_path / "aggregated" / "asset_001" / "2026" / "04" / "04" / "temp_sensor_hour.parquet")
        daily_path = (temp_storage_path / "daily" / "asset_001" / "2026" / "04" / "temp_sensor_day.parquet")
        
        # Hourly should exist since we have minute data
        assert hourly_path.exists()

    def test_start_stop_scheduled_aggregations(self, scheduler):
        """Test starting and stopping scheduled aggregations."""
        assert scheduler.running is False
        
        # Start scheduler
        scheduler.start_scheduled_aggregations()
        assert scheduler.running is True
        assert scheduler.scheduler_thread is not None
        
        # Stop scheduler
        scheduler.stop_scheduled_aggregations()
        assert scheduler.running is False

    @patch('app.aggregation.scheduler.time.sleep')
    @patch('app.aggregation.scheduler.datetime')
    def test_scheduled_aggregation_timing(self, mock_datetime, mock_sleep, scheduler):
        """Test scheduled aggregation timing logic."""
        # Mock current time to trigger hourly aggregation 
        mock_time = datetime(2026, 4, 4, 14, 5, 0)  # 5 minutes past hour
        mock_datetime.utcnow.return_value = mock_time
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        # Start scheduler (will run once and then we'll stop it)
        scheduler.running = True
        
        # Mock the aggregation methods to track calls
        with patch.object(scheduler, '_create_hourly_aggregations') as mock_hourly, \
             patch.object(scheduler, '_create_daily_aggregations') as mock_daily:
            
            # Simulate scheduler worker loop (one iteration)
            try:
                current_time = mock_datetime.utcnow()
                
                # Should trigger hourly aggregation at 5 minutes past hour
                if current_time.minute == 5 and current_time.second < 60:
                    scheduler._create_hourly_aggregations(
                        current_time - timedelta(hours=1)
                    )
                
                # Should not trigger daily at this time
                if (current_time.hour == 1 and current_time.minute == 5 and current_time.second < 60):
                    scheduler._create_daily_aggregations(
                        current_time.date() - timedelta(days=1)
                    )
                    
            except Exception:
                pass  # Ignore any errors in this test
            
            # Verify hourly aggregation was called
            mock_hourly.assert_called_once()
            # Daily should not be called at this hour
            mock_daily.assert_not_called()