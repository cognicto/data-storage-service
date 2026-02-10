"""
FastAPI routes for the sensor data storage service.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Global references to services (set by main.py)
kafka_consumer = None
storage_manager = None
azure_uploader = None
cleanup_service = None


def create_app() -> FastAPI:
    """Create FastAPI application with routes."""
    app = FastAPI(
        title="Sensor Data Storage Service",
        description="Microservice for consuming sensor data from Kafka, storing hierarchically, and uploading to Azure",
        version="1.0.0"
    )
    
    @app.get("/health")
    async def health_check() -> Dict:
        """Health check endpoint."""
        try:
            overall_healthy = True
            components = {}
            
            # Check Kafka consumer
            if kafka_consumer:
                kafka_health = kafka_consumer.health_check()
                components['kafka'] = kafka_health
                if not kafka_health['healthy']:
                    overall_healthy = False
            else:
                components['kafka'] = {'healthy': False, 'issues': ['Not initialized']}
                overall_healthy = False
            
            # Check storage manager
            if storage_manager:
                try:
                    storage_stats = storage_manager.get_storage_stats()
                    components['storage'] = {
                        'healthy': True,
                        'stats': storage_stats
                    }
                except Exception as e:
                    components['storage'] = {'healthy': False, 'issues': [str(e)]}
                    overall_healthy = False
            else:
                components['storage'] = {'healthy': False, 'issues': ['Not initialized']}
                overall_healthy = False
            
            # Check Azure uploader
            if azure_uploader:
                azure_health = azure_uploader.health_check()
                components['azure'] = azure_health
                # Azure is optional, don't fail overall health if not configured
            else:
                components['azure'] = {'healthy': False, 'issues': ['Not initialized']}
            
            # Check cleanup service
            if cleanup_service:
                cleanup_health = cleanup_service.health_check()
                components['cleanup'] = cleanup_health
                if not cleanup_health['healthy']:
                    overall_healthy = False
            else:
                components['cleanup'] = {'healthy': False, 'issues': ['Not initialized']}
            
            return {
                'status': 'healthy' if overall_healthy else 'unhealthy',
                'timestamp': '2024-01-01T00:00:00Z',  # Will be replaced with actual timestamp
                'components': components
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return JSONResponse(
                status_code=500,
                content={
                    'status': 'unhealthy',
                    'error': str(e)
                }
            )
    
    @app.get("/metrics")
    async def get_metrics() -> Dict:
        """Get service metrics."""
        try:
            metrics = {}
            
            # Kafka metrics
            if kafka_consumer:
                metrics['kafka'] = kafka_consumer.get_metrics()
            
            # Storage metrics
            if storage_manager:
                metrics['storage'] = storage_manager.get_storage_stats()
            
            # Azure metrics
            if azure_uploader:
                metrics['azure'] = azure_uploader.get_metrics()
            
            # Cleanup metrics
            if cleanup_service:
                metrics['cleanup'] = cleanup_service.get_metrics()
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/upload/trigger")
    async def trigger_upload(background_tasks: BackgroundTasks) -> Dict:
        """Manually trigger Azure upload."""
        try:
            if not azure_uploader:
                raise HTTPException(status_code=503, detail="Azure uploader not available")
            
            if not storage_manager:
                raise HTTPException(status_code=503, detail="Storage manager not available")
            
            # Get all local files
            local_files = storage_manager.get_local_files()
            
            if not local_files:
                return {
                    'status': 'completed',
                    'message': 'No files to upload',
                    'files_processed': 0
                }
            
            # Trigger upload in background
            background_tasks.add_task(run_upload_task, local_files)
            
            return {
                'status': 'started',
                'message': f'Upload of {len(local_files)} files started in background',
                'files_to_process': len(local_files)
            }
            
        except Exception as e:
            logger.error(f"Failed to trigger upload: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/cleanup/trigger")
    async def trigger_cleanup(background_tasks: BackgroundTasks) -> Dict:
        """Manually trigger file cleanup."""
        try:
            if not cleanup_service:
                raise HTTPException(status_code=503, detail="Cleanup service not available")
            
            # Trigger cleanup in background
            background_tasks.add_task(run_cleanup_task)
            
            return {
                'status': 'started',
                'message': 'Cleanup started in background'
            }
            
        except Exception as e:
            logger.error(f"Failed to trigger cleanup: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/storage/flush")
    async def flush_storage_buffers() -> Dict:
        """Flush all storage buffers to disk."""
        try:
            if not storage_manager:
                raise HTTPException(status_code=503, detail="Storage manager not available")
            
            flushed_files = storage_manager.flush_all_buffers()
            
            return {
                'status': 'completed',
                'files_flushed': len(flushed_files),
                'files': [str(f) for f in flushed_files]
            }
            
        except Exception as e:
            logger.error(f"Failed to flush buffers: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/storage/stats")
    async def get_storage_stats() -> Dict:
        """Get detailed storage statistics."""
        try:
            if not storage_manager:
                raise HTTPException(status_code=503, detail="Storage manager not available")
            
            return storage_manager.get_storage_stats()
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/azure/files")
    async def list_azure_files(prefix: Optional[str] = None) -> Dict:
        """List files in Azure Blob Storage."""
        try:
            if not azure_uploader:
                raise HTTPException(status_code=503, detail="Azure uploader not available")
            
            files = azure_uploader.list_azure_files(prefix or "")
            
            return {
                'files': files,
                'count': len(files),
                'total_size_mb': sum(f['size_mb'] for f in files)
            }
            
        except Exception as e:
            logger.error(f"Failed to list Azure files: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    return app


async def run_upload_task(files):
    """Background task for uploading files."""
    try:
        logger.info(f"Starting background upload of {len(files)} files")
        results = azure_uploader.upload_files_parallel(files)
        
        # Mark uploaded files for cleanup
        if cleanup_service:
            uploaded_files = [result['local_file'] for result in results['uploaded']]
            cleanup_service.mark_files_uploaded([Path(f) for f in uploaded_files])
        
        logger.info(f"Background upload completed: {len(results['uploaded'])} uploaded, {len(results['failed'])} failed")
        
    except Exception as e:
        logger.error(f"Background upload task failed: {e}")


async def run_cleanup_task():
    """Background task for file cleanup."""
    try:
        logger.info("Starting background cleanup")
        result = cleanup_service.run_cleanup()
        logger.info(f"Background cleanup completed: {result}")
        
    except Exception as e:
        logger.error(f"Background cleanup task failed: {e}")


def set_service_references(consumer, storage, uploader, cleanup):
    """Set global references to service components."""
    global kafka_consumer, storage_manager, azure_uploader, cleanup_service
    kafka_consumer = consumer
    storage_manager = storage
    azure_uploader = uploader
    cleanup_service = cleanup