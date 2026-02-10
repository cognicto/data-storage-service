"""
Kafka consumer for sensor data ingestion.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Callable
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, Message
import re

from app.config import KafkaConfig

logger = logging.getLogger(__name__)


class SensorDataConsumer:
    """Kafka consumer for sensor data with regex topic subscription."""
    
    def __init__(self, config: KafkaConfig):
        """Initialize the Kafka consumer."""
        self.config = config
        self.consumer = None
        self.running = False
        self.message_handler: Optional[Callable[[Dict], None]] = None
        
        # Metrics
        self.messages_consumed = 0
        self.messages_failed = 0
        self.last_message_time = None
        
    def connect(self) -> bool:
        """Connect to Kafka cluster."""
        try:
            consumer_conf = {
                'bootstrap.servers': self.config.bootstrap_servers,
                'group.id': self.config.consumer_group,
                'auto.offset.reset': self.config.auto_offset_reset,
                'enable.auto.commit': self.config.enable_auto_commit,
                'session.timeout.ms': self.config.session_timeout_ms,
                'max.poll.interval.ms': self.config.max_poll_interval_ms
            }
            
            self.consumer = Consumer(consumer_conf)
            
            # Subscribe to topics using regex pattern
            self.consumer.subscribe([self.config.topic_pattern])
            
            logger.info(f"Connected to Kafka with pattern: {self.config.topic_pattern}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def set_message_handler(self, handler: Callable[[Dict], None]):
        """Set the message handler function."""
        self.message_handler = handler
    
    def parse_message(self, message: Message) -> Optional[Dict]:
        """Parse Kafka message into structured data."""
        try:
            # Extract topic information
            topic = message.topic()
            partition = message.partition()
            offset = message.offset()
            
            # Parse message value
            if message.value() is None:
                logger.warning(f"Received null message from {topic}")
                return None
            
            # Decode message
            try:
                data = json.loads(message.value().decode('utf-8'))
            except json.JSONDecodeError:
                # Try parsing as string for simple messages
                data = {"value": message.value().decode('utf-8')}
            
            # Extract sensor information from topic name
            # Expected format: sensor-data-quad_ch1, sensor-data-quad_ch2, etc.
            sensor_match = re.search(r'sensor-data-(.+)', topic)
            if sensor_match:
                sensor_name = sensor_match.group(1)
            else:
                sensor_name = topic
            
            # Enrich message with metadata
            enriched_data = {
                'sensor_name': sensor_name,
                'topic': topic,
                'partition': partition,
                'offset': offset,
                'timestamp': datetime.utcnow().isoformat(),
                'kafka_timestamp': message.timestamp()[1] if message.timestamp()[0] != -1 else None,
                'data': data
            }
            
            return enriched_data
            
        except Exception as e:
            logger.error(f"Failed to parse message from {message.topic()}: {e}")
            return None
    
    def start_consuming(self, poll_timeout: float = 1.0):
        """Start consuming messages from Kafka."""
        if not self.consumer:
            if not self.connect():
                raise RuntimeError("Failed to connect to Kafka")
        
        if not self.message_handler:
            raise RuntimeError("Message handler not set")
        
        self.running = True
        logger.info("Starting Kafka message consumption")
        
        try:
            while self.running:
                message = self.consumer.poll(timeout=poll_timeout)
                
                if message is None:
                    continue
                
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {message.partition()}")
                    else:
                        logger.error(f"Consumer error: {message.error()}")
                    continue
                
                # Process message
                try:
                    parsed_data = self.parse_message(message)
                    if parsed_data:
                        self.message_handler(parsed_data)
                        self.messages_consumed += 1
                        self.last_message_time = datetime.utcnow()
                        
                        # Commit offset manually if auto-commit is disabled
                        if not self.config.enable_auto_commit:
                            self.consumer.commit(asynchronous=False)
                        
                        if self.messages_consumed % 100 == 0:
                            logger.info(f"Processed {self.messages_consumed} messages")
                    
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    self.messages_failed += 1
                    
                    # Still commit to avoid reprocessing the same bad message
                    if not self.config.enable_auto_commit:
                        self.consumer.commit(asynchronous=False)
        
        except KeyboardInterrupt:
            logger.info("Consumption interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error during consumption: {e}")
            raise
        finally:
            self.stop_consuming()
    
    def stop_consuming(self):
        """Stop consuming messages and cleanup."""
        self.running = False
        if self.consumer:
            logger.info("Closing Kafka consumer")
            self.consumer.close()
            self.consumer = None
    
    def get_metrics(self) -> Dict:
        """Get consumer metrics."""
        return {
            'messages_consumed': self.messages_consumed,
            'messages_failed': self.messages_failed,
            'last_message_time': self.last_message_time.isoformat() if self.last_message_time else None,
            'is_running': self.running
        }
    
    def health_check(self) -> Dict:
        """Perform health check."""
        healthy = True
        issues = []
        
        # Check if consumer is connected
        if not self.consumer:
            healthy = False
            issues.append("Not connected to Kafka")
        
        # Check if we've received messages recently (within 5 minutes)
        if self.last_message_time:
            time_since_last = (datetime.utcnow() - self.last_message_time).total_seconds()
            if time_since_last > 300:  # 5 minutes
                issues.append(f"No messages received for {time_since_last:.0f} seconds")
        
        return {
            'healthy': healthy,
            'issues': issues,
            'metrics': self.get_metrics()
        }