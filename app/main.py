"""
Main application entry point for the sensor data storage service.
"""

import asyncio
import logging
import signal
import sys
import threading
import time
import uvicorn
from datetime import datetime
from pathlib import Path

from app.config import load_config, validate_config
from app.kafka.consumer import SensorDataConsumer
from app.storage.manager import HierarchicalStorageManager
from app.azure.uploader import AzureBlobUploader
from app.cleanup.service import FileCleanupService
from app.aggregation.scheduler import AggregationScheduler
from app.api.routes import create_app, set_service_references

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/sensor-storage-service.log')
    ]
)

logger = logging.getLogger(__name__)


class SensorDataStorageService:
    """Main service class that orchestrates all components."""
    
    def __init__(self):
        """Initialize the service."""
        self.config = None
        self.kafka_consumer = None
        self.storage_manager = None
        self.azure_uploader = None
        self.cleanup_service = None
        self.aggregation_scheduler = None
        self.api_server = None
        self.running = False
        
        # Background tasks
        self.upload_thread = None
        self.kafka_thread = None
        
    def initialize(self) -> bool:
        """Initialize all service components."""
        try:
            # Load and validate configuration
            logger.info("Loading configuration...")
            self.config = load_config()
            
            if not validate_config(self.config):
                logger.error("Configuration validation failed")
                return False
            
            # Initialize storage manager
            logger.info("Initializing storage manager...")
            self.storage_manager = HierarchicalStorageManager(self.config.storage)
            
            # Initialize Azure uploader
            logger.info("Initializing Azure uploader...")
            self.azure_uploader = AzureBlobUploader(
                self.config.azure, 
                self.config.storage.local_path
            )
            
            # Initialize cleanup service
            logger.info("Initializing cleanup service...")
            self.cleanup_service = FileCleanupService(
                self.config.cleanup,
                self.config.storage.local_path
            )
            
            # Initialize aggregation scheduler
            logger.info("Initializing aggregation scheduler...")
            self.aggregation_scheduler = AggregationScheduler(
                self.config.storage.local_path,
                self.config.storage.parquet_compression
            )
            
            # Initialize Kafka consumer
            logger.info("Initializing Kafka consumer...")
            self.kafka_consumer = SensorDataConsumer(self.config.kafka)
            self.kafka_consumer.set_message_handler(self.handle_kafka_message)
            
            # Set API service references
            set_service_references(
                self.kafka_consumer,
                self.storage_manager,
                self.azure_uploader,
                self.cleanup_service
            )
            
            logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize service: {e}")
            return False
    
    def handle_kafka_message(self, data: dict):
        """Handle incoming Kafka message."""
        try:
            # Store data locally
            file_path = self.storage_manager.add_to_buffer(data)
            
            # If a file was flushed, consider uploading it
            if file_path and self.azure_uploader:
                # Upload in background if configured
                if hasattr(self, '_should_upload_immediately') and self._should_upload_immediately:
                    success = self.azure_uploader.upload_single_file(file_path)
                    if success and self.cleanup_service:
                        self.cleanup_service.mark_file_uploaded(file_path)
            
        except Exception as e:
            logger.error(f"Failed to handle Kafka message: {e}")
    
    def start_kafka_consumer(self):
        """Start Kafka consumer in background thread."""
        def kafka_worker():
            try:
                logger.info("Starting Kafka consumer...")
                self.kafka_consumer.start_consuming()
            except Exception as e:
                logger.error(f"Kafka consumer failed: {e}")
        
        self.kafka_thread = threading.Thread(target=kafka_worker, daemon=True)
        self.kafka_thread.start()
    
    def start_upload_scheduler(self):
        """Start periodic upload scheduler."""
        def upload_worker():
            while self.running:
                try:
                    # Wait for upload interval (default: every 30 minutes)
                    upload_interval = 1800  # 30 minutes
                    
                    # Sleep in small intervals to allow quick shutdown
                    for _ in range(upload_interval):
                        if not self.running:
                            break
                        time.sleep(1)
                    
                    if not self.running:
                        break
                    
                    # Flush any pending storage buffers
                    flushed_files = self.storage_manager.flush_all_buffers()
                    if flushed_files:
                        logger.info(f"Flushed {len(flushed_files)} files before upload")
                    
                    # Get all local files and upload them
                    local_files = self.storage_manager.get_local_files()
                    if local_files:
                        logger.info(f"Starting scheduled upload of {len(local_files)} files")
                        results = self.azure_uploader.upload_files_parallel(local_files)
                        
                        # Mark uploaded files for cleanup
                        uploaded_files = [result['local_file'] for result in results['uploaded']]
                        if uploaded_files and self.cleanup_service:
                            self.cleanup_service.mark_files_uploaded([Path(f) for f in uploaded_files])
                            logger.info(f"Marked {len(uploaded_files)} files for cleanup")
                    
                except Exception as e:
                    logger.error(f"Upload scheduler error: {e}")
                    time.sleep(60)  # Wait before retrying
        
        self.upload_thread = threading.Thread(target=upload_worker, daemon=True)
        self.upload_thread.start()
    
    def start_api_server(self):
        """Start the FastAPI server."""
        app = create_app()
        
        # Run server in background thread
        def run_server():
            uvicorn.run(
                app,
                host=self.config.service.host,
                port=self.config.service.port,
                workers=1,  # Single worker for simplicity
                log_level="info"
            )
        
        self.api_thread = threading.Thread(target=run_server, daemon=True)
        self.api_thread.start()
        logger.info(f"API server started on {self.config.service.host}:{self.config.service.port}")
    
    def start(self):
        """Start the complete service."""
        try:
            if not self.initialize():
                logger.error("Service initialization failed")
                return False
            
            self.running = True
            
            # Start cleanup service
            self.cleanup_service.start_scheduled_cleanup()
            
            # Start aggregation scheduler
            self.aggregation_scheduler.start_scheduled_aggregations()
            
            # Start Kafka consumer
            self.start_kafka_consumer()
            
            # Start upload scheduler
            self.start_upload_scheduler()
            
            # Start API server
            self.start_api_server()
            
            logger.info("Sensor Data Storage Service started successfully")
            logger.info(f"  - Kafka topics: {self.config.kafka.topic_pattern}")
            logger.info(f"  - Local storage: {self.config.storage.local_path}")
            logger.info(f"  - Azure container: {self.config.azure.container_name}")
            logger.info(f"  - Cleanup enabled: {self.config.cleanup.enabled}")
            logger.info(f"  - API endpoint: http://{self.config.service.host}:{self.config.service.port}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start service: {e}")
            return False
    
    def stop(self):
        """Stop the service gracefully."""
        logger.info("Stopping Sensor Data Storage Service...")
        
        self.running = False
        
        # Stop Kafka consumer
        if self.kafka_consumer:
            self.kafka_consumer.stop_consuming()
        
        # Stop cleanup service
        if self.cleanup_service:
            self.cleanup_service.stop_scheduled_cleanup()
        
        # Stop aggregation scheduler
        if self.aggregation_scheduler:
            self.aggregation_scheduler.stop_scheduled_aggregations()
        
        # Flush any remaining data
        if self.storage_manager:
            flushed_files = self.storage_manager.flush_all_buffers()
            if flushed_files:
                logger.info(f"Flushed {len(flushed_files)} files during shutdown")
            
            # Flush minute aggregation buffers
            self.storage_manager.flush_all_minute_buffers()
            logger.info("Flushed all minute aggregation buffers")
        
        # Wait for threads to finish
        for thread in [self.kafka_thread, self.upload_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("Service stopped")
    
    def run_forever(self):
        """Run the service until interrupted."""
        if not self.start():
            sys.exit(1)
        
        # Setup signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()


def main():
    """Main entry point."""
    logger.info("Starting Sensor Data Storage Service...")
    
    service = SensorDataStorageService()
    service.run_forever()


if __name__ == "__main__":
    main()