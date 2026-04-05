"""
Scheduled aggregation service for creating hourly and daily aggregations.
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class AggregationScheduler:
    """Handles scheduled creation of hourly and daily aggregations."""

    def __init__(self, storage_path: Path, compression: str = "snappy"):
        """Initialize aggregation scheduler."""
        self.storage_path = storage_path
        self.compression = compression
        self.running = False
        self.scheduler_thread = None

        logger.info(f"Initialized aggregation scheduler for: {storage_path}")

    def needs_reaggregation(self, aggregation_file: Path, source_files: List[Path]) -> bool:
        """Check if aggregation needs update based on source file modification times."""
        if not aggregation_file.exists():
            return True
        
        try:
            agg_mtime = aggregation_file.stat().st_mtime
            for source_file in source_files:
                if source_file.exists() and source_file.stat().st_mtime > agg_mtime:
                    logger.info(f"Reaggregation needed: {source_file} newer than {aggregation_file}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking file modification times: {e}")
            return True  # Re-aggregate on error to be safe

    def start_scheduled_aggregations(self):
        """Start scheduled aggregation processing."""
        if self.running:
            return

        self.running = True

        def scheduler_worker():
            logger.info("Starting aggregation scheduler...")

            while self.running:
                try:
                    current_time = datetime.utcnow()

                    # Run hourly aggregation at 5 minutes past each hour
                    if current_time.minute == 5 and current_time.second < 60:
                        self._create_hourly_aggregations(
                            current_time - timedelta(hours=1)
                        )

                    # Run daily aggregation at 1:05 AM each day
                    if (
                        current_time.hour == 1
                        and current_time.minute == 5
                        and current_time.second < 60
                    ):
                        self._create_daily_aggregations(
                            current_time.date() - timedelta(days=1)
                        )

                    # Sleep for 1 minute
                    for _ in range(60):
                        if not self.running:
                            break
                        time.sleep(1)

                except Exception as e:
                    logger.error(f"Aggregation scheduler error: {e}")
                    time.sleep(60)

        self.scheduler_thread = threading.Thread(target=scheduler_worker, daemon=True)
        self.scheduler_thread.start()
        logger.info("Aggregation scheduler started")

    def stop_scheduled_aggregations(self):
        """Stop scheduled aggregation processing."""
        if not self.running:
            return

        logger.info("Stopping aggregation scheduler...")
        self.running = False

        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=10)

        logger.info("Aggregation scheduler stopped")

    def _create_hourly_aggregations(self, target_hour: datetime):
        """Create hourly aggregations from minute-level data."""
        try:
            logger.info(
                f"Creating hourly aggregations for {target_hour.strftime('%Y-%m-%d %H:00')}"
            )

            # Find all minute aggregation files for the target hour
            hour_path = (
                self.storage_path
                / "aggregated"
                / "*"  # asset_id wildcard
                / f"{target_hour.year:04d}"
                / f"{target_hour.month:02d}"
                / f"{target_hour.day:02d}"
                / f"{target_hour.hour:02d}"
            )

            # Process each asset
            for asset_dir in (self.storage_path / "aggregated").iterdir():
                if not asset_dir.is_dir():
                    continue

                asset_hour_path = (
                    asset_dir
                    / f"{target_hour.year:04d}"
                    / f"{target_hour.month:02d}"
                    / f"{target_hour.day:02d}"
                    / f"{target_hour.hour:02d}"
                )

                if not asset_hour_path.exists():
                    continue

                # Process each sensor
                for minute_file in asset_hour_path.glob("*_minute.parquet"):
                    sensor_name = minute_file.stem.replace("_minute", "")
                    
                    # Check if hourly aggregation needs update
                    hourly_path = (
                        self.storage_path
                        / "aggregated"
                        / asset_dir.name
                        / f"{target_hour.year:04d}"
                        / f"{target_hour.month:02d}"
                        / f"{target_hour.day:02d}"
                        / f"{sensor_name}_hour.parquet"
                    )
                    
                    if self.needs_reaggregation(hourly_path, [minute_file]):
                        logger.info(f"Updating hourly aggregation due to modified minute file: {minute_file}")
                        self._aggregate_minute_to_hour(
                            minute_file, sensor_name, asset_dir.name, target_hour
                        )
                    else:
                        logger.debug(f"Hourly aggregation up-to-date: {hourly_path}")

            logger.info(
                f"Completed hourly aggregations for {target_hour.strftime('%Y-%m-%d %H:00')}"
            )

        except Exception as e:
            logger.error(f"Failed to create hourly aggregations: {e}")

    def _aggregate_minute_to_hour(
        self, minute_file: Path, sensor_name: str, asset_id: str, target_hour: datetime
    ):
        """Aggregate minute data to hourly."""
        try:
            # Read minute aggregation data
            df = pd.read_parquet(minute_file)

            if df.empty:
                return

            # Filter for target hour
            df["minute_bucket"] = pd.to_datetime(df["minute_bucket"])
            hour_data = df[df["minute_bucket"].dt.hour == target_hour.hour]

            if hour_data.empty:
                return

            # Aggregate numeric columns
            numeric_columns = hour_data.select_dtypes(include=["number"]).columns
            numeric_columns = [
                col for col in numeric_columns if "timestamp" not in col.lower()
            ]

            if not numeric_columns:
                return

            # Create simple hourly aggregation 
            hourly_agg = pd.DataFrame({
                'value_mean': [hour_data[numeric_columns].mean().iloc[0]],
                'value_min': [hour_data[numeric_columns].min().iloc[0]],  
                'value_max': [hour_data[numeric_columns].max().iloc[0]],
                'record_count': [hour_data['record_count'].sum() if 'record_count' in hour_data.columns else len(hour_data)],
                'minute_count': [len(hour_data)],
                'timestamp_start': [hour_data['timestamp_start'].iloc[0] if 'timestamp_start' in hour_data.columns else hour_data.index[0]],
                'timestamp_end': [hour_data['timestamp_end'].iloc[-1] if 'timestamp_end' in hour_data.columns else hour_data.index[-1]]
            })

            # Add hour bucket
            hourly_agg["hour_bucket"] = target_hour.strftime("%Y-%m-%d %H:00:00")

            # Create hourly file path: aggregated/asset_id/yyyy/mm/dd/sensor_hour.parquet
            hourly_path = (
                self.storage_path
                / "aggregated"
                / asset_id
                / f"{target_hour.year:04d}"
                / f"{target_hour.month:02d}"
                / f"{target_hour.day:02d}"
                / f"{sensor_name}_hour.parquet"
            )

            # Save hourly aggregation (always overwrite to ensure latest data)
            hourly_path.parent.mkdir(parents=True, exist_ok=True)
            hourly_agg.to_parquet(hourly_path, compression=self.compression, index=False)

            logger.debug(f"Created hourly aggregation: {hourly_path}")

        except Exception as e:
            logger.error(f"Failed to aggregate {minute_file} to hourly: {e}")

    def _create_daily_aggregations(self, target_date):
        """Create daily aggregations from hourly data."""
        try:
            logger.info(f"Creating daily aggregations for {target_date}")

            # Process each asset
            for asset_dir in (self.storage_path / "aggregated").iterdir():
                if not asset_dir.is_dir():
                    continue

                asset_date_path = (
                    asset_dir
                    / f"{target_date.year:04d}"
                    / f"{target_date.month:02d}"
                    / f"{target_date.day:02d}"
                )

                if not asset_date_path.exists():
                    continue

                # Group hourly files by sensor
                sensor_files = {}
                for hourly_file in asset_date_path.glob("*_hour.parquet"):
                    sensor_name = hourly_file.stem.replace("_hour", "")
                    if sensor_name not in sensor_files:
                        sensor_files[sensor_name] = []
                    sensor_files[sensor_name].append(hourly_file)

                # Create daily aggregations for each sensor
                for sensor_name, files in sensor_files.items():
                    # Check if daily aggregation needs update
                    daily_path = (
                        self.storage_path
                        / "daily"
                        / asset_dir.name
                        / f"{target_date.year:04d}"
                        / f"{target_date.month:02d}"
                        / f"{sensor_name}_day.parquet"
                    )
                    
                    if self.needs_reaggregation(daily_path, files):
                        logger.info(f"Updating daily aggregation due to modified hourly files: {files}")
                        self._aggregate_hour_to_day(
                            files, sensor_name, asset_dir.name, target_date
                        )
                    else:
                        logger.debug(f"Daily aggregation up-to-date: {daily_path}")

            logger.info(f"Completed daily aggregations for {target_date}")

        except Exception as e:
            logger.error(f"Failed to create daily aggregations: {e}")

    def _aggregate_hour_to_day(
        self, hourly_files: List[Path], sensor_name: str, asset_id: str, target_date
    ):
        """Aggregate hourly data to daily."""
        try:
            if not hourly_files:
                return

            # Read all hourly files for the day
            daily_data = []
            for hourly_file in hourly_files:
                df = pd.read_parquet(hourly_file)
                if not df.empty:
                    daily_data.append(df)

            if not daily_data:
                return

            # Combine all hourly data
            combined_df = pd.concat(daily_data, ignore_index=True)

            # Aggregate numeric columns
            numeric_columns = combined_df.select_dtypes(include=["number"]).columns
            numeric_columns = [
                col for col in numeric_columns if "timestamp" not in col.lower()
            ]

            if not numeric_columns:
                return

            # Create simple daily aggregation
            daily_agg = pd.DataFrame({
                'value_mean': [combined_df[numeric_columns].mean().iloc[0]],
                'value_min': [combined_df[numeric_columns].min().iloc[0]],
                'value_max': [combined_df[numeric_columns].max().iloc[0]],
                'record_count': [combined_df['record_count'].sum() if 'record_count' in combined_df.columns else len(combined_df)],
                'minute_count': [combined_df['minute_count'].sum() if 'minute_count' in combined_df.columns else 0],
                'hour_count': [len(combined_df)],
                'timestamp_start': [combined_df['timestamp_start'].iloc[0] if 'timestamp_start' in combined_df.columns else combined_df.index[0]],
                'timestamp_end': [combined_df['timestamp_end'].iloc[-1] if 'timestamp_end' in combined_df.columns else combined_df.index[-1]]
            })

            # Add day bucket
            daily_agg["day_bucket"] = target_date.strftime("%Y-%m-%d")

            # Create daily file path: daily/asset_id/yyyy/mm/sensor_day.parquet
            daily_path = (
                self.storage_path
                / "daily"
                / asset_id
                / f"{target_date.year:04d}"
                / f"{target_date.month:02d}"
                / f"{sensor_name}_day.parquet"
            )

            # Save daily aggregation (always overwrite to ensure latest data)
            daily_path.parent.mkdir(parents=True, exist_ok=True)
            daily_agg.to_parquet(daily_path, compression=self.compression, index=False)

            logger.debug(f"Created daily aggregation: {daily_path}")

        except Exception as e:
            logger.error(
                f"Failed to aggregate hourly files to daily for {sensor_name}: {e}"
            )

    def force_create_aggregations(
        self, target_datetime: datetime, aggregation_type: str = "both"
    ):
        """Manually trigger aggregation creation."""
        try:
            if aggregation_type in ["hourly", "both"]:
                self._create_hourly_aggregations(target_datetime)

            if aggregation_type in ["daily", "both"]:
                self._create_daily_aggregations(target_datetime.date())

        except Exception as e:
            logger.error(f"Failed to force create aggregations: {e}")
