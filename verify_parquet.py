#!/usr/bin/env python3
"""
Parquet file verification script for sensor data storage service.
"""

import pandas as pd
import sys
from pathlib import Path
import argparse


def verify_parquet(file_path, show_sample=False, sample_size=5):
    """Verify and analyze a parquet file."""
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            print(f"❌ Error: File does not exist: {file_path}")
            return
        
        if not file_path.suffix.lower() == '.parquet':
            print(f"⚠️  Warning: File doesn't have .parquet extension: {file_path}")
        
        print(f"🔍 Analyzing: {file_path}")
        print("=" * 60)
        
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        # Basic file info
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        print(f"📁 File: {file_path.name}")
        print(f"📊 Records: {len(df):,}")
        print(f"🏷️  Columns: {len(df.columns)}")
        print(f"💾 File size: {file_size_mb:.2f} MB")
        
        # Memory usage
        memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"🧠 Memory usage: {memory_usage_mb:.2f} MB")
        
        print("\n" + "=" * 60)
        
        # Time analysis
        timestamp_column = None
        for col in ['timestamp', 'minute_bucket', 'hour_bucket', 'day_bucket']:
            if col in df.columns:
                timestamp_column = col
                break
        
        if timestamp_column:
            try:
                # Try different parsing methods for timestamps
                original_col = df[timestamp_column].copy()
                try:
                    # First try standard pandas parsing
                    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
                except:
                    try:
                        # Try ISO8601 format
                        df[timestamp_column] = pd.to_datetime(df[timestamp_column], format='ISO8601')
                    except:
                        try:
                            # Try mixed format
                            df[timestamp_column] = pd.to_datetime(df[timestamp_column], format='mixed')
                        except:
                            # If all parsing fails, show raw values and skip time analysis
                            print(f"⚠️  Could not parse {timestamp_column} column as datetime")
                            print(f"   Sample values: {original_col.head(3).tolist()}")
                            df[timestamp_column] = original_col  # Restore original
                            timestamp_column = None
                
                if timestamp_column and pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
                    time_min = df[timestamp_column].min()
                    time_max = df[timestamp_column].max()
                    time_range = time_max - time_min
                    
                    print(f"⏰ Time range:")
                    print(f"   Start: {time_min}")
                    print(f"   End:   {time_max}")
                    print(f"   Duration: {time_range}")
                    
                    # Check for time gaps (if more than 2 seconds between consecutive readings)
                    if timestamp_column == 'timestamp':  # Only for raw data
                        df_sorted = df.sort_values(timestamp_column)
                        time_diffs = df_sorted[timestamp_column].diff().dt.total_seconds()
                        large_gaps = time_diffs[time_diffs > 2].dropna()
                        
                        if len(large_gaps) > 0:
                            print(f"⚠️  Time gaps detected: {len(large_gaps)} gaps > 2 seconds")
                            print(f"   Largest gap: {large_gaps.max():.1f} seconds")
                        else:
                            print(f"✅ Time consistency: No significant gaps detected")
                            
            except Exception as e:
                print(f"⚠️  Error analyzing timestamps: {e}")
        
        # Value analysis
        numeric_columns = df.select_dtypes(include=['number']).columns
        if len(numeric_columns) > 0:
            print(f"\n📈 Numeric data analysis:")
            for col in numeric_columns:
                if col not in ['partition', 'offset', 'kafka_timestamp', 'time']:
                    values = df[col].dropna()
                    if len(values) > 0:
                        print(f"   {col}:")
                        print(f"     Range: {values.min():.3f} → {values.max():.3f}")
                        print(f"     Mean:  {values.mean():.3f}")
                        print(f"     Std:   {values.std():.3f}")
        
        # Asset info from file path (since it's no longer stored in records)
        try:
            path_parts = file_path.parts
            # Look for asset pattern in path: asset_001/2024/04/03/14/
            for i, part in enumerate(path_parts):
                if part.startswith('asset_'):
                    asset_id = part
                    if i + 4 < len(path_parts):
                        year, month, day, hour = path_parts[i+1:i+5]
                        print(f"\n🏭 Asset ID: {asset_id}")
                        print(f"📅 Date/Hour: {year}-{month}-{day} {hour}:00")
                    break
            else:
                print(f"\n📁 File path: {file_path.parent}")
        except Exception:
            print(f"\n📁 File path: {file_path.parent}")
        
        # Check for legacy sensor info (in aggregated files)
        if 'sensor_name' in df.columns:
            sensors = df['sensor_name'].unique()
            print(f"🎛️  Sensors: {list(sensors)}")
        
        if 'asset_id' in df.columns:
            assets = df['asset_id'].unique()
            print(f"🏭 Assets: {list(assets)}")
        
        # Column info
        print(f"\n🗂️  Column details:")
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].notna().sum()
            null_pct = ((len(df) - non_null) / len(df) * 100) if len(df) > 0 else 0
            print(f"   {col:20} {str(dtype):15} ({non_null:,} values, {null_pct:.1f}% null)")
        
        # Show sample data if requested
        if show_sample:
            print(f"\n📋 Sample data (first {sample_size} records):")
            print("-" * 80)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(df.head(sample_size).to_string())
            pd.reset_option('display.max_columns')
            pd.reset_option('display.width')
        
        print("\n✅ Analysis complete!")
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False
    
    return True


def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description='Verify and analyze Parquet files from sensor data storage service',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python verify_parquet.py /data/raw/asset_001/2024/04/03/14/quad_ch1_20240403_14.parquet
  python verify_parquet.py /data/aggregated/asset_001/2024/04/03/14/quad_ch1_minute.parquet --sample
  python verify_parquet.py /path/to/file.parquet --sample --sample-size 10
        '''
    )
    
    parser.add_argument('file_path', help='Path to the Parquet file to verify')
    parser.add_argument('--sample', action='store_true', 
                       help='Show sample data from the file')
    parser.add_argument('--sample-size', type=int, default=5,
                       help='Number of sample records to show (default: 5)')
    
    args = parser.parse_args()
    
    # Verify the file
    success = verify_parquet(args.file_path, args.sample, args.sample_size)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()