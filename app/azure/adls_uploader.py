"""
Azure Data Lake Storage Gen2 uploader with retry logic and parallel uploads.
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from azure.core.exceptions import AzureError
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    FileSystemClient,
)

from app.config import AzureConfig

logger = logging.getLogger(__name__)


class AzureDataLakeUploader:
    """Handles uploading files to Azure Data Lake Storage Gen2."""

    def __init__(self, config: AzureConfig, local_storage_path: Path):
        """Initialize ADLS Gen2 uploader."""
        self.config = config
        self.local_storage_path = local_storage_path
        self.datalake_service_client = None
        self.file_system_client = None

        # Initialize ADLS Gen2 connection using connection string
        if config.connection_string:
            try:
                self.datalake_service_client = DataLakeServiceClient.from_connection_string(
                    config.connection_string
                )
                
                # Get or create file system client
                self.file_system_client = self.datalake_service_client.get_file_system_client(
                    config.file_system_name
                )
                
                # Extract storage account from connection string for logging
                self._extract_storage_account_name()
                
                logger.info(
                    f"Connected to ADLS Gen2 file system '{config.file_system_name}' using connection string"
                )
                
                # Ensure file system exists
                self._ensure_file_system_exists()
                
            except Exception as e:
                logger.error(f"Failed to connect to ADLS Gen2: {e}")
                self.datalake_service_client = None
                self.file_system_client = None
        else:
            logger.warning(
                "ADLS Gen2 connection string not configured - uploads will be disabled"
            )
            self.datalake_service_client = None
            self.file_system_client = None

        # Metrics
        self.upload_stats = {
            "total_uploads": 0,
            "successful_uploads": 0,
            "failed_uploads": 0,
            "skipped_uploads": 0,
            "total_bytes": 0,
            "last_upload_time": None,
        }

    def _extract_storage_account_name(self):
        """Extract storage account name from connection string."""
        try:
            # Parse AccountName from connection string
            match = re.search(r"AccountName=([^;]+)", self.config.connection_string)
            if match:
                self.storage_account_name = match.group(1)
                logger.info(f"Extracted storage account: {self.storage_account_name}")
            else:
                self.storage_account_name = "unknown"
        except Exception as e:
            logger.warning(f"Could not extract storage account name: {e}")
            self.storage_account_name = "unknown"

    def _ensure_file_system_exists(self):
        """Ensure the ADLS Gen2 file system exists."""
        if not self.file_system_client:
            return

        try:
            if not self.file_system_client.exists():
                try:
                    self.file_system_client.create_file_system()
                    logger.info(
                        f"Created ADLS Gen2 file system: {self.config.file_system_name}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not create file system (might already exist or insufficient permissions): {e}"
                    )
            else:
                logger.info(f"ADLS Gen2 file system exists: {self.config.file_system_name}")
        except AzureError as e:
            logger.warning(f"Could not verify file system existence: {e}")

    def get_file_path(self, local_file: Path) -> str:
        """Convert local file path to ADLS Gen2 path maintaining hierarchy."""
        try:
            rel_path = local_file.relative_to(self.local_storage_path)
            # Convert to ADLS path (use forward slashes)
            adls_path = str(rel_path).replace("\\", "/")
            return adls_path
        except ValueError:
            # File is not under local storage path
            logger.warning(
                f"File {local_file} is not under storage path {self.local_storage_path}"
            )
            return local_file.name

    def file_needs_upload(self, local_file: Path, adls_path: str) -> bool:
        """Check if local file needs to be uploaded."""
        if not self.file_system_client:
            return False

        try:
            file_client = self.file_system_client.get_file_client(adls_path)

            if not file_client.exists():
                return True

            # Get file properties
            file_props = file_client.get_file_properties()
            adls_modified = file_props.last_modified.replace(tzinfo=None)

            # Get local file modification time
            local_modified = datetime.fromtimestamp(local_file.stat().st_mtime)

            # Upload if local file is newer or size differs significantly
            local_size = local_file.stat().st_size
            adls_size = file_props.size

            if local_modified > adls_modified or abs(local_size - adls_size) > 1024:
                return True

            return False

        except AzureError as e:
            logger.debug(f"Could not check file status for {adls_path}: {e}")
            # If we can't check, assume upload is needed
            return True

    def upload_file(self, local_file: Path) -> Dict[str, any]:
        """Upload a single file to ADLS Gen2 with retry logic."""
        if not self.file_system_client:
            return {
                "local_file": str(local_file),
                "adls_path": "",
                "status": "skipped",
                "reason": "adls_not_configured",
                "size_mb": local_file.stat().st_size / (1024 * 1024),
            }

        adls_path = self.get_file_path(local_file)

        try:
            # Check if upload is needed
            if not self.file_needs_upload(local_file, adls_path):
                self.upload_stats["skipped_uploads"] += 1
                return {
                    "local_file": str(local_file),
                    "adls_path": adls_path,
                    "status": "skipped",
                    "reason": "already_up_to_date",
                    "size_mb": local_file.stat().st_size / (1024 * 1024),
                }

            # Create directory if needed
            directory_path = str(Path(adls_path).parent).replace("\\", "/")
            if directory_path != ".":
                try:
                    directory_client = self.file_system_client.get_directory_client(directory_path)
                    if not directory_client.exists():
                        directory_client.create_directory()
                except Exception as e:
                    logger.debug(f"Directory creation info for {directory_path}: {e}")

            # Get file client
            file_client = self.file_system_client.get_file_client(adls_path)

            # Upload with retries
            last_error = None
            for attempt in range(self.config.max_retries):
                try:
                    with open(local_file, "rb") as data:
                        # Create file and upload data
                        file_client.create_file()
                        file_client.upload_data(
                            data, 
                            overwrite=True,
                            max_concurrency=self.config.max_workers
                        )

                    # Verify upload succeeded
                    if file_client.exists():
                        file_size_mb = local_file.stat().st_size / (1024 * 1024)
                        self.upload_stats["successful_uploads"] += 1
                        self.upload_stats["total_bytes"] += local_file.stat().st_size
                        self.upload_stats["last_upload_time"] = datetime.utcnow()

                        logger.info(
                            f"Successfully uploaded to ADLS Gen2: {adls_path} ({file_size_mb:.2f} MB)"
                        )

                        return {
                            "local_file": str(local_file),
                            "adls_path": adls_path,
                            "status": "uploaded",
                            "size_mb": file_size_mb,
                            "attempts": attempt + 1,
                        }

                except Exception as e:
                    last_error = e
                    if attempt < self.config.max_retries - 1:
                        wait_time = self.config.retry_delay * (
                            2**attempt
                        )  # Exponential backoff
                        logger.warning(
                            f"Upload attempt {attempt + 1} failed for {adls_path}, retrying in {wait_time}s: {e}"
                        )
                        time.sleep(wait_time)

            # All retries failed
            self.upload_stats["failed_uploads"] += 1
            logger.error(
                f"Failed to upload {adls_path} after {self.config.max_retries} attempts: {last_error}"
            )
            return {
                "local_file": str(local_file),
                "adls_path": adls_path,
                "status": "failed",
                "error": str(last_error),
                "size_mb": local_file.stat().st_size / (1024 * 1024),
                "attempts": self.config.max_retries,
            }

        except Exception as e:
            self.upload_stats["failed_uploads"] += 1
            logger.error(f"Unexpected error uploading {local_file}: {e}")
            return {
                "local_file": str(local_file),
                "adls_path": adls_path,
                "status": "failed",
                "error": str(e),
                "size_mb": local_file.stat().st_size / (1024 * 1024),
                "attempts": 1,
            }

    def upload_files_parallel(self, files: List[Path]) -> Dict[str, List]:
        """Upload multiple files in parallel."""
        if not files:
            return {"uploaded": [], "skipped": [], "failed": []}

        if not self.file_system_client:
            logger.warning("ADLS Gen2 not configured - skipping uploads")
            return {
                "uploaded": [],
                "skipped": [{"reason": "adls_not_configured", "files": len(files)}],
                "failed": [],
            }

        results = {"uploaded": [], "skipped": [], "failed": []}

        total_files = len(files)
        logger.info(
            f"Starting ADLS Gen2 upload of {total_files} files with {self.config.max_workers} workers"
        )

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all upload tasks
            future_to_file = {
                executor.submit(self.upload_file, file): file for file in files
            }

            # Process completed uploads
            completed = 0
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    status = result["status"]

                    # Add to appropriate result category
                    if status == "uploaded":
                        results["uploaded"].append(result)
                    elif status == "skipped":
                        results["skipped"].append(result)
                    else:
                        results["failed"].append(result)

                    completed += 1
                    if completed % 10 == 0 or completed == total_files:
                        logger.info(
                            f"ADLS Gen2 upload progress: {completed}/{total_files} files processed"
                        )

                    if status == "uploaded":
                        logger.debug(
                            f"Uploaded to ADLS Gen2: {result['adls_path']} ({result['size_mb']:.2f} MB)"
                        )
                    elif status == "failed":
                        logger.error(
                            f"Failed to upload {result['local_file']}: {result.get('error', 'Unknown error')}"
                        )

                except Exception as e:
                    logger.error(f"Unexpected error processing upload result: {e}")
                    completed += 1

        # Update total stats
        self.upload_stats["total_uploads"] += total_files

        logger.info(
            f"ADLS Gen2 upload complete - Uploaded: {len(results['uploaded'])}, Skipped: {len(results['skipped'])}, Failed: {len(results['failed'])}"
        )

        return results

    def upload_single_file(self, file_path: Path) -> bool:
        """Upload a single file and return success status."""
        result = self.upload_file(file_path)
        return result["status"] == "uploaded"

    def list_adls_files(self, prefix: str = "") -> List[Dict]:
        """List files in ADLS Gen2 file system with optional prefix filter."""
        if not self.file_system_client:
            return []

        try:
            files = []
            paths = self.file_system_client.get_paths(
                path=prefix if prefix else None, recursive=True
            )

            for path_item in paths:
                if not path_item.is_directory:
                    # Get detailed file properties
                    file_client = self.file_system_client.get_file_client(path_item.name)
                    try:
                        props = file_client.get_file_properties()
                        files.append(
                            {
                                "name": path_item.name,
                                "size_mb": props.size / (1024 * 1024),
                                "last_modified": props.last_modified,
                                "content_type": props.content_settings.content_type if props.content_settings else None,
                            }
                        )
                    except Exception as e:
                        # If we can't get properties, add basic info
                        files.append(
                            {
                                "name": path_item.name,
                                "size_mb": 0,
                                "last_modified": None,
                                "content_type": None,
                                "error": str(e),
                            }
                        )

            return files

        except AzureError as e:
            logger.error(f"Failed to list ADLS Gen2 files: {e}")
            return []

    def get_metrics(self) -> Dict:
        """Get uploader metrics."""
        return {
            "upload_stats": self.upload_stats.copy(),
            "adls_configured": self.file_system_client is not None,
            "file_system_name": self.config.file_system_name,
            "storage_account": getattr(self, 'storage_account_name', 'unknown'),
        }

    def health_check(self) -> Dict:
        """Perform health check."""
        healthy = True
        issues = []

        # Check if ADLS Gen2 is configured
        if not self.file_system_client:
            healthy = False
            issues.append("ADLS Gen2 not configured")
        else:
            # Test connection
            try:
                # Try to get file system properties
                self.file_system_client.get_file_system_properties()
                logger.info("ADLS Gen2 connection test successful")
            except Exception as e:
                # Try a simpler operation - list a single path
                try:
                    next(self.file_system_client.get_paths(max_results=1), None)
                    logger.info("ADLS Gen2 connection test successful (limited permissions)")
                except Exception as e2:
                    healthy = False
                    issues.append(f"Cannot connect to ADLS Gen2: {e2}")

        return {"healthy": healthy, "issues": issues, "metrics": self.get_metrics()}