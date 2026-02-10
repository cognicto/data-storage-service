"""
Azure Blob Storage uploader with retry logic and parallel uploads.
"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import AzureError

from app.config import AzureConfig

logger = logging.getLogger(__name__)


class AzureBlobUploader:
    """Handles uploading files to Azure Blob Storage."""
    
    def __init__(self, config: AzureConfig, local_storage_path: Path):
        """Initialize Azure uploader."""
        self.config = config
        self.local_storage_path = local_storage_path
        
        if not config.storage_account or not config.storage_key:
            logger.warning("Azure credentials not configured - uploads will be disabled")
            self.blob_service_client = None
        else:
            self.blob_service_client = BlobServiceClient(
                account_url=f"https://{config.storage_account}.blob.core.windows.net",
                credential=config.storage_key
            )
            self._ensure_container_exists()
        
        # Metrics
        self.upload_stats = {
            'total_uploads': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'skipped_uploads': 0,
            'total_bytes': 0,
            'last_upload_time': None
        }
    
    def _ensure_container_exists(self):
        """Ensure the Azure container exists."""
        if not self.blob_service_client:
            return
            
        try:
            container_client = self.blob_service_client.get_container_client(self.config.container_name)
            if not container_client.exists():
                container_client.create_container()
                logger.info(f"Created Azure container: {self.config.container_name}")
        except AzureError as e:
            logger.error(f"Failed to create/check container: {e}")
            raise
    
    def get_blob_path(self, local_file: Path) -> str:
        """Convert local file path to blob path maintaining hierarchy."""
        try:
            rel_path = local_file.relative_to(self.local_storage_path)
            # Convert to blob path (use forward slashes)
            blob_path = str(rel_path).replace('\\', '/')
            return blob_path
        except ValueError:
            # File is not under local storage path
            logger.warning(f"File {local_file} is not under storage path {self.local_storage_path}")
            return local_file.name
    
    def file_needs_upload(self, local_file: Path, blob_path: str) -> bool:
        """Check if local file needs to be uploaded."""
        if not self.blob_service_client:
            return False
            
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.config.container_name, 
                blob=blob_path
            )
            
            if not blob_client.exists():
                return True
                
            # Get blob properties
            blob_props = blob_client.get_blob_properties()
            blob_modified = blob_props.last_modified.replace(tzinfo=None)
            
            # Get local file modification time
            local_modified = datetime.fromtimestamp(local_file.stat().st_mtime)
            
            # Upload if local file is newer or same size differs significantly
            local_size = local_file.stat().st_size
            blob_size = blob_props.size
            
            if local_modified > blob_modified or abs(local_size - blob_size) > 1024:
                return True
                
            return False
            
        except AzureError as e:
            logger.debug(f"Could not check blob status for {blob_path}: {e}")
            # If we can't check, assume upload is needed
            return True
    
    def upload_file(self, local_file: Path) -> Dict[str, any]:
        """Upload a single file to Azure Blob Storage with retry logic."""
        if not self.blob_service_client:
            return {
                'local_file': str(local_file),
                'blob_path': '',
                'status': 'skipped',
                'reason': 'azure_not_configured',
                'size_mb': local_file.stat().st_size / (1024 * 1024)
            }
        
        blob_path = self.get_blob_path(local_file)
        
        try:
            # Check if upload is needed
            if not self.file_needs_upload(local_file, blob_path):
                self.upload_stats['skipped_uploads'] += 1
                return {
                    'local_file': str(local_file),
                    'blob_path': blob_path,
                    'status': 'skipped',
                    'reason': 'already_up_to_date',
                    'size_mb': local_file.stat().st_size / (1024 * 1024)
                }
            
            # Upload with retries
            blob_client = self.blob_service_client.get_blob_client(
                container=self.config.container_name, 
                blob=blob_path
            )
            
            last_error = None
            for attempt in range(self.config.max_retries):
                try:
                    with open(local_file, 'rb') as data:
                        blob_client.upload_blob(data, overwrite=True)
                    
                    # Verify upload succeeded
                    if blob_client.exists():
                        file_size_mb = local_file.stat().st_size / (1024 * 1024)
                        self.upload_stats['successful_uploads'] += 1
                        self.upload_stats['total_bytes'] += local_file.stat().st_size
                        self.upload_stats['last_upload_time'] = datetime.utcnow()
                        
                        return {
                            'local_file': str(local_file),
                            'blob_path': blob_path,
                            'status': 'uploaded',
                            'size_mb': file_size_mb,
                            'attempts': attempt + 1
                        }
                        
                except Exception as e:
                    last_error = e
                    if attempt < self.config.max_retries - 1:
                        wait_time = self.config.retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Upload attempt {attempt + 1} failed for {blob_path}, retrying in {wait_time}s: {e}")
                        time.sleep(wait_time)
            
            # All retries failed
            self.upload_stats['failed_uploads'] += 1
            return {
                'local_file': str(local_file),
                'blob_path': blob_path,
                'status': 'failed',
                'error': str(last_error),
                'size_mb': local_file.stat().st_size / (1024 * 1024),
                'attempts': self.config.max_retries
            }
            
        except Exception as e:
            self.upload_stats['failed_uploads'] += 1
            return {
                'local_file': str(local_file),
                'blob_path': blob_path,
                'status': 'failed',
                'error': str(e),
                'size_mb': local_file.stat().st_size / (1024 * 1024),
                'attempts': 1
            }
    
    def upload_files_parallel(self, files: List[Path]) -> Dict[str, List]:
        """Upload multiple files in parallel."""
        if not files:
            return {'uploaded': [], 'skipped': [], 'failed': []}
        
        if not self.blob_service_client:
            logger.warning("Azure not configured - skipping uploads")
            return {
                'uploaded': [],
                'skipped': [{'reason': 'azure_not_configured', 'files': len(files)}],
                'failed': []
            }
        
        results = {
            'uploaded': [],
            'skipped': [],
            'failed': []
        }
        
        total_files = len(files)
        logger.info(f"Starting upload of {total_files} files with {self.config.max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all upload tasks
            future_to_file = {
                executor.submit(self.upload_file, file): file 
                for file in files
            }
            
            # Process completed uploads
            completed = 0
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    status = result['status']
                    results[status].append(result)
                    
                    completed += 1
                    if completed % 10 == 0 or completed == total_files:
                        logger.info(f"Upload progress: {completed}/{total_files} files processed")
                        
                    if status == 'uploaded':
                        logger.info(f"Uploaded: {result['blob_path']} ({result['size_mb']:.2f} MB)")
                    elif status == 'failed':
                        logger.error(f"Failed to upload {result['local_file']}: {result['error']}")
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing upload result: {e}")
                    completed += 1
        
        # Update total stats
        self.upload_stats['total_uploads'] += total_files
        
        return results
    
    def upload_single_file(self, file_path: Path) -> bool:
        """Upload a single file and return success status."""
        result = self.upload_file(file_path)
        return result['status'] == 'uploaded'
    
    def list_azure_files(self, prefix: str = "") -> List[Dict]:
        """List files in Azure container with optional prefix filter."""
        if not self.blob_service_client:
            return []
            
        try:
            container_client = self.blob_service_client.get_container_client(self.config.container_name)
            blobs = []
            
            for blob in container_client.list_blobs(name_starts_with=prefix):
                blobs.append({
                    'name': blob.name,
                    'size_mb': blob.size / (1024 * 1024),
                    'last_modified': blob.last_modified,
                    'content_type': blob.content_settings.content_type if blob.content_settings else None
                })
            
            return blobs
            
        except AzureError as e:
            logger.error(f"Failed to list Azure files: {e}")
            return []
    
    def get_metrics(self) -> Dict:
        """Get uploader metrics."""
        return {
            'upload_stats': self.upload_stats.copy(),
            'azure_configured': self.blob_service_client is not None,
            'container_name': self.config.container_name
        }
    
    def health_check(self) -> Dict:
        """Perform health check."""
        healthy = True
        issues = []
        
        # Check if Azure is configured
        if not self.blob_service_client:
            healthy = False
            issues.append("Azure Blob Storage not configured")
        else:
            # Test connection
            try:
                container_client = self.blob_service_client.get_container_client(self.config.container_name)
                container_client.get_container_properties()
            except Exception as e:
                healthy = False
                issues.append(f"Cannot connect to Azure: {e}")
        
        return {
            'healthy': healthy,
            'issues': issues,
            'metrics': self.get_metrics()
        }