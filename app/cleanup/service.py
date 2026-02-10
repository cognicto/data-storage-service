"""
File cleanup service for managing local storage.
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set
import threading

from app.config import CleanupConfig

logger = logging.getLogger(__name__)


class FileCleanupService:
    """Service for cleaning up local files after successful Azure upload."""
    
    def __init__(self, config: CleanupConfig, local_storage_path: Path):
        """Initialize cleanup service."""
        self.config = config
        self.local_storage_path = local_storage_path
        self.running = False
        self.cleanup_thread = None
        
        # Track successfully uploaded files
        self._uploaded_files: Set[str] = set()
        self._upload_lock = threading.Lock()
        
        # Metrics
        self.cleanup_stats = {
            'total_files_deleted': 0,
            'total_bytes_freed': 0,
            'last_cleanup_time': None,
            'cleanup_runs': 0,
            'directories_removed': 0
        }
        
        logger.info(f"Initialized cleanup service - enabled: {config.enabled}")
    
    def mark_file_uploaded(self, file_path: Path):
        """Mark a file as successfully uploaded to Azure."""
        with self._upload_lock:
            self._uploaded_files.add(str(file_path.absolute()))
        logger.debug(f"Marked file as uploaded: {file_path}")
    
    def mark_files_uploaded(self, file_paths: List[Path]):
        """Mark multiple files as successfully uploaded."""
        with self._upload_lock:
            for file_path in file_paths:
                self._uploaded_files.add(str(file_path.absolute()))
        logger.info(f"Marked {len(file_paths)} files as uploaded")
    
    def is_file_uploaded(self, file_path: Path) -> bool:
        """Check if a file has been marked as uploaded."""
        with self._upload_lock:
            return str(file_path.absolute()) in self._uploaded_files
    
    def get_files_to_cleanup(self) -> List[Path]:
        """Get list of files eligible for cleanup."""
        if not self.config.enabled:
            return []
        
        cutoff_time = datetime.now() - timedelta(days=self.config.age_days)
        eligible_files = []
        
        try:
            # Find all parquet files
            for file_path in self.local_storage_path.rglob("*.parquet"):
                if not file_path.is_file():
                    continue
                
                # Check file age
                file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_modified >= cutoff_time:
                    continue
                
                # Check if file was successfully uploaded
                if self.is_file_uploaded(file_path):
                    eligible_files.append(file_path)
                else:
                    logger.debug(f"File not uploaded yet, skipping: {file_path}")
            
            logger.info(f"Found {len(eligible_files)} files eligible for cleanup")
            return eligible_files
            
        except Exception as e:
            logger.error(f"Error finding files to cleanup: {e}")
            return []
    
    def cleanup_file(self, file_path: Path) -> Dict[str, any]:
        """Clean up a single file."""
        try:
            if not file_path.exists():
                return {
                    'file': str(file_path),
                    'status': 'skipped',
                    'reason': 'file_not_found',
                    'size_mb': 0
                }
            
            file_size = file_path.stat().st_size
            file_size_mb = file_size / (1024 * 1024)
            
            if self.config.dry_run:
                return {
                    'file': str(file_path),
                    'status': 'would_delete',
                    'size_mb': file_size_mb
                }
            
            # Delete the file
            file_path.unlink()
            
            # Remove from uploaded files set
            with self._upload_lock:
                self._uploaded_files.discard(str(file_path.absolute()))
            
            # Update metrics
            self.cleanup_stats['total_files_deleted'] += 1
            self.cleanup_stats['total_bytes_freed'] += file_size
            
            logger.info(f"Cleaned up file: {file_path} ({file_size_mb:.2f} MB)")
            
            return {
                'file': str(file_path),
                'status': 'deleted',
                'size_mb': file_size_mb
            }
            
        except Exception as e:
            logger.error(f"Failed to cleanup file {file_path}: {e}")
            return {
                'file': str(file_path),
                'status': 'failed',
                'error': str(e),
                'size_mb': 0
            }
    
    def cleanup_empty_directories(self, start_path: Path = None) -> int:
        """Remove empty directories recursively."""
        if start_path is None:
            start_path = self.local_storage_path
        
        if self.config.dry_run:
            return 0
        
        removed_count = 0
        
        try:
            # Get all directories, sorted by depth (deepest first)
            all_dirs = [d for d in start_path.rglob("*") if d.is_dir()]
            all_dirs.sort(key=lambda x: len(x.parts), reverse=True)
            
            for directory in all_dirs:
                # Don't remove the root storage directory
                if directory == self.local_storage_path:
                    continue
                
                try:
                    # Check if directory is empty
                    if directory.exists() and not any(directory.iterdir()):
                        directory.rmdir()
                        removed_count += 1
                        logger.debug(f"Removed empty directory: {directory}")
                except OSError:
                    # Directory not empty or other error, skip
                    continue
            
            if removed_count > 0:
                self.cleanup_stats['directories_removed'] += removed_count
                logger.info(f"Removed {removed_count} empty directories")
            
        except Exception as e:
            logger.error(f"Error cleaning up directories: {e}")
        
        return removed_count
    
    def run_cleanup(self) -> Dict[str, any]:
        """Run a complete cleanup cycle."""
        if not self.config.enabled:
            return {
                'status': 'disabled',
                'files_processed': 0,
                'files_deleted': 0,
                'bytes_freed': 0
            }
        
        start_time = time.time()
        logger.info("Starting file cleanup cycle")
        
        try:
            # Get files to cleanup
            files_to_cleanup = self.get_files_to_cleanup()
            
            if not files_to_cleanup:
                logger.info("No files to cleanup")
                return {
                    'status': 'completed',
                    'files_processed': 0,
                    'files_deleted': 0,
                    'bytes_freed': 0,
                    'duration_seconds': time.time() - start_time
                }
            
            # Process files
            deleted_count = 0
            failed_count = 0
            bytes_freed = 0
            
            for file_path in files_to_cleanup:
                result = self.cleanup_file(file_path)
                if result['status'] == 'deleted':
                    deleted_count += 1
                    bytes_freed += result['size_mb'] * 1024 * 1024
                elif result['status'] == 'failed':
                    failed_count += 1
            
            # Cleanup empty directories
            dirs_removed = self.cleanup_empty_directories()
            
            # Update metrics
            self.cleanup_stats['cleanup_runs'] += 1
            self.cleanup_stats['last_cleanup_time'] = datetime.utcnow()
            
            duration = time.time() - start_time
            
            result = {
                'status': 'completed',
                'files_processed': len(files_to_cleanup),
                'files_deleted': deleted_count,
                'files_failed': failed_count,
                'bytes_freed': bytes_freed,
                'directories_removed': dirs_removed,
                'duration_seconds': duration
            }
            
            logger.info(f"Cleanup completed: {deleted_count} files deleted, "
                       f"{bytes_freed / (1024 * 1024):.2f} MB freed in {duration:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Cleanup cycle failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'duration_seconds': time.time() - start_time
            }
    
    def start_scheduled_cleanup(self):
        """Start the scheduled cleanup service."""
        if not self.config.enabled:
            logger.info("Cleanup service disabled")
            return
        
        if self.running:
            logger.warning("Cleanup service already running")
            return
        
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        logger.info(f"Started scheduled cleanup service (interval: {self.config.check_interval_hours}h)")
    
    def stop_scheduled_cleanup(self):
        """Stop the scheduled cleanup service."""
        self.running = False
        if self.cleanup_thread and self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=5)
        logger.info("Stopped scheduled cleanup service")
    
    def _cleanup_loop(self):
        """Main cleanup loop running in background thread."""
        while self.running:
            try:
                # Wait for the next cleanup time
                sleep_time = self.config.check_interval_hours * 3600  # Convert to seconds
                
                # Sleep in small intervals to allow for quick shutdown
                for _ in range(int(sleep_time)):
                    if not self.running:
                        break
                    time.sleep(1)
                
                if not self.running:
                    break
                
                # Run cleanup
                self.run_cleanup()
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                time.sleep(60)  # Wait a minute before retrying
    
    def get_metrics(self) -> Dict:
        """Get cleanup service metrics."""
        with self._upload_lock:
            uploaded_files_count = len(self._uploaded_files)
        
        return {
            'cleanup_stats': self.cleanup_stats.copy(),
            'config': {
                'enabled': self.config.enabled,
                'age_days': self.config.age_days,
                'check_interval_hours': self.config.check_interval_hours,
                'dry_run': self.config.dry_run
            },
            'uploaded_files_tracked': uploaded_files_count,
            'is_running': self.running
        }
    
    def health_check(self) -> Dict:
        """Perform health check."""
        healthy = True
        issues = []
        
        # Check if storage path exists
        if not self.local_storage_path.exists():
            healthy = False
            issues.append(f"Storage path does not exist: {self.local_storage_path}")
        
        # Check if cleanup is enabled but not running
        if self.config.enabled and not self.running:
            issues.append("Cleanup enabled but not running")
        
        return {
            'healthy': healthy,
            'issues': issues,
            'metrics': self.get_metrics()
        }