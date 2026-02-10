"""
Local storage manager for hierarchical Parquet file storage.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

from app.config import StorageConfig

logger = logging.getLogger(__name__)


class HierarchicalStorageManager:
    """Manages hierarchical storage of sensor data in Parquet format."""
    
    def __init__(self, config: StorageConfig):
        """Initialize the storage manager."""
        self.config = config
        self.local_path = config.local_path
        self.compression = config.parquet_compression
        self.max_rows = config.max_rows_per_file
        
        # Create base directory
        self.local_path.mkdir(parents=True, exist_ok=True)
        
        # Create aggregation directories
        (self.local_path / "aggregated").mkdir(exist_ok=True)
        (self.local_path / "daily").mkdir(exist_ok=True)
        
        # File buffers for batching
        self._file_buffers: Dict[str, List[Dict]] = {}
        self._buffer_timestamps: Dict[str, datetime] = {}
        
        # Aggregation buffers for real-time aggregation
        self._minute_buffers: Dict[str, List[Dict]] = {}
        self._minute_buffer_timestamps: Dict[str, datetime] = {}
        
        logger.info(f"Initialized storage manager at: {self.local_path}")
    
    def extract_metadata(self, data: Dict) -> Dict[str, str]:
        """Extract metadata for file organization."""
        try:
            # Get timestamp
            timestamp_str = data.get('timestamp')
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.utcnow()
            
            # Extract asset_id from data (fallback to sensor_name if not available)
            asset_id = data.get('data', {}).get('asset_id', 'default_asset')
            if not asset_id or asset_id == '':
                asset_id = f"asset_{data.get('sensor_name', 'unknown')}"
            
            # Extract sensor name
            sensor_name = data.get('sensor_name', 'unknown_sensor')
            
            return {
                'asset_id': asset_id,
                'sensor_name': sensor_name,
                'year': f"{timestamp.year:04d}",
                'month': f"{timestamp.month:02d}",
                'day': f"{timestamp.day:02d}",
                'hour': f"{timestamp.hour:02d}",
                'timestamp': timestamp.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to extract metadata: {e}")
            # Return default metadata
            now = datetime.utcnow()
            return {
                'asset_id': 'error_asset',
                'sensor_name': data.get('sensor_name', 'unknown_sensor'),
                'year': f"{now.year:04d}",
                'month': f"{now.month:02d}",
                'day': f"{now.day:02d}",
                'hour': f"{now.hour:02d}",
                'timestamp': now.isoformat()
            }
    
    def get_file_path(self, metadata: Dict[str, str]) -> Path:
        """Generate hierarchical file path based on metadata."""
        # Structure: asset_id/yyyy/mm/dd/hh/sensor_name.parquet
        file_path = (
            self.local_path /
            metadata['asset_id'] /
            metadata['year'] /
            metadata['month'] /
            metadata['day'] /
            metadata['hour'] /
            f"{metadata['sensor_name']}.parquet"
        )
        return file_path
    
    def get_buffer_key(self, metadata: Dict[str, str]) -> str:
        """Generate buffer key for grouping messages."""
        return f"{metadata['asset_id']}/{metadata['year']}/{metadata['month']}/{metadata['day']}/{metadata['hour']}/{metadata['sensor_name']}"
    
    def add_to_buffer(self, data: Dict) -> Optional[Path]:
        """Add data to buffer and return file path if buffer should be flushed."""
        try:
            metadata = self.extract_metadata(data)
            buffer_key = self.get_buffer_key(metadata)
            
            # Initialize buffer if needed
            if buffer_key not in self._file_buffers:
                self._file_buffers[buffer_key] = []
                self._buffer_timestamps[buffer_key] = datetime.utcnow()
            
            # Flatten data structure for easier processing
            flattened_data = {
                'sensor_name': metadata['sensor_name'],
                'asset_id': metadata['asset_id'],
                'timestamp': metadata['timestamp'],
                'topic': data.get('topic'),
                'partition': data.get('partition'),
                'offset': data.get('offset'),
                'kafka_timestamp': data.get('kafka_timestamp')
            }
            
            # Add data fields
            if isinstance(data.get('data'), dict):
                for key, value in data['data'].items():
                    # Flatten nested structures
                    if isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            flattened_data[f"{key}_{subkey}"] = subvalue
                    else:
                        flattened_data[key] = value
            else:
                flattened_data['value'] = data.get('data')
            
            self._file_buffers[buffer_key].append(flattened_data)
            
            # Add to minute aggregation buffer
            self._add_to_minute_buffer(flattened_data, metadata)
            
            # Check if buffer should be flushed
            if len(self._file_buffers[buffer_key]) >= self.max_rows:
                return self.flush_buffer(buffer_key)
            
            # Check if buffer is old (flush after 5 minutes)
            buffer_age = (datetime.utcnow() - self._buffer_timestamps[buffer_key]).total_seconds()
            if buffer_age > 300:  # 5 minutes
                return self.flush_buffer(buffer_key)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to add data to buffer: {e}")
            return None
    
    def flush_buffer(self, buffer_key: str) -> Optional[Path]:
        """Flush buffer to Parquet file."""
        try:
            if buffer_key not in self._file_buffers or not self._file_buffers[buffer_key]:
                return None
            
            # Get data from buffer
            data_list = self._file_buffers[buffer_key]
            
            # Create DataFrame
            df = pd.DataFrame(data_list)
            
            # Get file path from first record
            first_record = data_list[0]
            metadata = {
                'asset_id': first_record['asset_id'],
                'sensor_name': first_record['sensor_name'],
                'year': first_record['timestamp'][:4],
                'month': first_record['timestamp'][5:7],
                'day': first_record['timestamp'][8:10],
                'hour': first_record['timestamp'][11:13]
            }
            
            file_path = self.get_file_path(metadata)
            
            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write or append to Parquet file
            if file_path.exists():
                # Read existing data and append
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_parquet(
                    file_path,
                    compression=self.compression,
                    index=False
                )
            else:
                # Create new file
                df.to_parquet(
                    file_path,
                    compression=self.compression,
                    index=False
                )
            
            # Clear buffer
            del self._file_buffers[buffer_key]
            del self._buffer_timestamps[buffer_key]
            
            logger.info(f"Flushed {len(data_list)} records to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to flush buffer {buffer_key}: {e}")
            return None
    
    def flush_all_buffers(self) -> List[Path]:
        """Flush all buffers to disk."""
        flushed_files = []
        buffer_keys = list(self._file_buffers.keys())
        
        for buffer_key in buffer_keys:
            file_path = self.flush_buffer(buffer_key)
            if file_path:
                flushed_files.append(file_path)
        
        logger.info(f"Flushed {len(flushed_files)} files")
        return flushed_files
    
    def get_local_files(self, pattern: str = "*.parquet") -> List[Path]:
        """Get all local Parquet files."""
        files = []
        for file_path in self.local_path.rglob(pattern):
            if file_path.is_file():
                files.append(file_path)
        return sorted(files)
    
    def get_storage_stats(self) -> Dict:
        """Get storage statistics."""
        files = self.get_local_files()
        total_size = sum(f.stat().st_size for f in files)
        
        # Group by asset_id
        asset_stats = {}
        for file_path in files:
            parts = file_path.relative_to(self.local_path).parts
            if len(parts) >= 1:
                asset_id = parts[0]
                if asset_id not in asset_stats:
                    asset_stats[asset_id] = {'files': 0, 'size_bytes': 0}
                asset_stats[asset_id]['files'] += 1
                asset_stats[asset_id]['size_bytes'] += file_path.stat().st_size
        
        return {
            'total_files': len(files),
            'total_size_bytes': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'assets': asset_stats,
            'buffer_count': len(self._file_buffers),
            'buffered_records': sum(len(buf) for buf in self._file_buffers.values()),
            'minute_buffer_count': len(self._minute_buffers),
            'minute_buffered_records': sum(len(buf) for buf in self._minute_buffers.values())
        }
    
    def _add_to_minute_buffer(self, flattened_data: Dict, metadata: Dict[str, str]):
        """Add data to minute-level aggregation buffer."""
        try:
            # Create minute-level buffer key (without seconds)
            minute_key = f"{metadata['asset_id']}/{metadata['year']}/{metadata['month']}/{metadata['day']}/{metadata['hour']}/{metadata['sensor_name']}_minute"
            
            if minute_key not in self._minute_buffers:
                self._minute_buffers[minute_key] = []
                self._minute_buffer_timestamps[minute_key] = datetime.utcnow()
            
            # Add only numeric fields for aggregation
            numeric_data = {}
            for key, value in flattened_data.items():
                if isinstance(value, (int, float)) and key not in ['partition', 'offset']:
                    numeric_data[key] = value
            
            if numeric_data:
                aggregated_record = {
                    'sensor_name': metadata['sensor_name'],
                    'asset_id': metadata['asset_id'],
                    'timestamp': flattened_data['timestamp'],
                    'minute_bucket': flattened_data['timestamp'][:16] + ":00",  # Round to minute
                    **numeric_data
                }
                self._minute_buffers[minute_key].append(aggregated_record)
            
            # Check if minute buffer should be flushed (every 2 minutes or 60 records)
            if (len(self._minute_buffers[minute_key]) >= 60 or 
                (datetime.utcnow() - self._minute_buffer_timestamps[minute_key]).total_seconds() > 120):
                self._flush_minute_buffer(minute_key)
                
        except Exception as e:
            logger.error(f"Failed to add data to minute buffer: {e}")
    
    def _flush_minute_buffer(self, minute_key: str):
        """Flush minute aggregation buffer."""
        try:
            if minute_key not in self._minute_buffers or not self._minute_buffers[minute_key]:
                return
            
            data_list = self._minute_buffers[minute_key]
            df = pd.DataFrame(data_list)
            
            # Group by minute bucket and aggregate
            numeric_columns = df.select_dtypes(include=['number']).columns
            numeric_columns = [col for col in numeric_columns if col not in ['timestamp']]
            
            if not numeric_columns:
                return
            
            # Aggregate by minute
            aggregated = df.groupby(['minute_bucket', 'sensor_name', 'asset_id']).agg({
                **{col: ['mean', 'min', 'max'] for col in numeric_columns},
                'timestamp': 'first'
            }).reset_index()
            
            # Flatten column names
            aggregated.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in aggregated.columns.values]
            
            # Get metadata from first record
            first_record = data_list[0]
            timestamp_obj = datetime.fromisoformat(first_record['timestamp'].replace('Z', '+00:00'))
            
            # Create aggregated file path: aggregated/asset_id/yyyy/mm/dd/hh/sensor_minute.parquet
            agg_path = (
                self.local_path / "aggregated" /
                first_record['asset_id'] /
                f"{timestamp_obj.year:04d}" /
                f"{timestamp_obj.month:02d}" /
                f"{timestamp_obj.day:02d}" /
                f"{timestamp_obj.hour:02d}" /
                f"{first_record['sensor_name']}_minute.parquet"
            )
            
            # Create directory and save
            agg_path.parent.mkdir(parents=True, exist_ok=True)
            
            if agg_path.exists():
                existing_df = pd.read_parquet(agg_path)
                combined_df = pd.concat([existing_df, aggregated], ignore_index=True)
                combined_df.to_parquet(agg_path, compression=self.compression, index=False)
            else:
                aggregated.to_parquet(agg_path, compression=self.compression, index=False)
            
            # Clear buffer
            del self._minute_buffers[minute_key]
            del self._minute_buffer_timestamps[minute_key]
            
            logger.info(f"Flushed minute aggregation to {agg_path}")
            
        except Exception as e:
            logger.error(f"Failed to flush minute buffer {minute_key}: {e}")
    
    def flush_all_minute_buffers(self):
        """Flush all minute aggregation buffers."""
        buffer_keys = list(self._minute_buffers.keys())
        for buffer_key in buffer_keys:
            self._flush_minute_buffer(buffer_key)