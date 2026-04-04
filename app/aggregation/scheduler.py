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
                    self._aggregate_minute_to_hour(
                        minute_file, sensor_name, asset_dir.name, target_hour
                    )

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

            # Group by hour and aggregate (no need to group by sensor/asset since file is already per-sensor)
            # Get timestamp columns if they exist (from enhanced minute aggregations)
            timestamp_cols = {}
            if "timestamp_start" in hour_data.columns:
                timestamp_cols["timestamp_start"] = ["first", "last"]
            if "timestamp_end" in hour_data.columns:
                timestamp_cols["timestamp_end"] = "last"
            if "record_count" in hour_data.columns:
                timestamp_cols["record_count"] = "sum"

            # Create aggregation dictionary
            agg_dict = {
                **{col: ["mean", "min", "max"] for col in numeric_columns},
                "minute_bucket": ["first", "count"],
                **timestamp_cols,
            }

            hourly_agg = hour_data.agg(agg_dict).to_frame().T

            # Flatten column names and rename for clarity
            new_columns = []
            for col in hourly_agg.columns.values:
                if col[1]:  # Multi-level column
                    if col[0] == "minute_bucket" and col[1] == "first":
                        new_columns.append("first_minute_bucket")
                    elif col[0] == "minute_bucket" and col[1] == "count":
                        new_columns.append("minute_count")
                    elif col[0] == "timestamp_start" and col[1] == "first":
                        new_columns.append("timestamp_start")
                    elif col[0] == "timestamp_start" and col[1] == "last":
                        new_columns.append("timestamp_end")
                    elif col[0] == "record_count" and col[1] == "sum":
                        new_columns.append("record_count")
                    else:
                        new_columns.append("_".join(col).strip("_"))
                else:  # Single-level column
                    new_columns.append(col[0])

            hourly_agg.columns = new_columns

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

            # Save hourly aggregation
            hourly_path.parent.mkdir(parents=True, exist_ok=True)

            if hourly_path.exists():
                existing_df = pd.read_parquet(hourly_path)
                combined_df = pd.concat([existing_df, hourly_agg], ignore_index=True)
                combined_df.to_parquet(
                    hourly_path, compression=self.compression, index=False
                )
            else:
                hourly_agg.to_parquet(
                    hourly_path, compression=self.compression, index=False
                )

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
                    self._aggregate_hour_to_day(
                        files, sensor_name, asset_dir.name, target_date
                    )

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

            # Aggregate without grouping by sensor/asset since file is per-sensor
            # Get timestamp and count columns if they exist (from enhanced hourly aggregations)
            timestamp_cols = {}
            if "timestamp_start" in combined_df.columns:
                timestamp_cols["timestamp_start"] = ["first", "last"]
            if "timestamp_end" in combined_df.columns:
                timestamp_cols["timestamp_end"] = "last"
            if "record_count" in combined_df.columns:
                timestamp_cols["record_count"] = "sum"
            if "minute_count" in combined_df.columns:
                timestamp_cols["minute_count"] = "sum"

            # Create aggregation dictionary
            agg_dict = {
                **{col: ["mean", "min", "max"] for col in numeric_columns},
                "hour_bucket": ["first", "count"],
                **timestamp_cols,
            }

            daily_agg = combined_df.agg(agg_dict).to_frame().T

            # Flatten column names and rename for clarity
            new_columns = []
            for col in daily_agg.columns.values:
                if col[1]:  # Multi-level column
                    if col[0] == "hour_bucket" and col[1] == "first":
                        new_columns.append("first_hour_bucket")
                    elif col[0] == "hour_bucket" and col[1] == "count":
                        new_columns.append("hour_count")
                    elif col[0] == "timestamp_start" and col[1] == "first":
                        new_columns.append("timestamp_start")
                    elif col[0] == "timestamp_start" and col[1] == "last":
                        new_columns.append("timestamp_end")
                    elif col[0] == "record_count" and col[1] == "sum":
                        new_columns.append("record_count")
                    elif col[0] == "minute_count" and col[1] == "sum":
                        new_columns.append("minute_count")
                    else:
                        new_columns.append("_".join(col).strip("_"))
                else:  # Single-level column
                    new_columns.append(col[0])

            daily_agg.columns = new_columns

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

            # Save daily aggregation
            daily_path.parent.mkdir(parents=True, exist_ok=True)

            if daily_path.exists():
                existing_df = pd.read_parquet(daily_path)
                combined_df = pd.concat([existing_df, daily_agg], ignore_index=True)
                combined_df.to_parquet(
                    daily_path, compression=self.compression, index=False
                )
            else:
                daily_agg.to_parquet(
                    daily_path, compression=self.compression, index=False
                )

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
