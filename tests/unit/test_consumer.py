"""Unit tests for Kafka consumer."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json

from app.kafka.consumer import SensorDataConsumer
from app.config import KafkaConfig


@pytest.fixture
def kafka_config():
    """Create test Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic_pattern="^sensor-data-.*",
        consumer_group="test-group",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        session_timeout_ms=30000,
        max_poll_interval_ms=300000,
        max_poll_records=500
    )


@pytest.fixture
def consumer(kafka_config):
    """Create consumer instance."""
    return SensorDataConsumer(kafka_config)


class TestSensorDataConsumer:
    """Test cases for SensorDataConsumer."""

    def test_initialization(self, consumer, kafka_config):
        """Test consumer initialization."""
        assert consumer.config == kafka_config
        assert consumer.consumer is None
        assert consumer.running is False
        assert consumer.messages_consumed == 0
        assert consumer.messages_failed == 0

    @patch('app.kafka.consumer.Consumer')
    def test_connect(self, mock_consumer_class, consumer):
        """Test Kafka connection."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        result = consumer.connect()
        
        assert result is True
        assert consumer.consumer == mock_consumer
        mock_consumer.subscribe.assert_called_once_with([consumer.config.topic_pattern])

    @patch('app.kafka.consumer.Consumer')
    def test_connect_failure(self, mock_consumer_class, consumer):
        """Test Kafka connection failure."""
        mock_consumer_class.side_effect = Exception("Connection failed")
        
        result = consumer.connect()
        
        assert result is False
        assert consumer.consumer is None

    def test_set_message_handler(self, consumer):
        """Test setting message handler."""
        handler = Mock()
        consumer.set_message_handler(handler)
        assert consumer.message_handler == handler

    def test_parse_message(self, consumer):
        """Test message parsing."""
        mock_message = Mock()
        mock_message.topic.return_value = "sensor-data-quad_ch1"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 100
        mock_message.value.return_value = json.dumps({"temperature": 25.5}).encode('utf-8')
        mock_message.timestamp.return_value = (1, 1704067200000)
        
        result = consumer.parse_message(mock_message)
        
        assert result is not None
        assert result['sensor_name'] == 'quad_ch1'
        assert result['topic'] == 'sensor-data-quad_ch1'
        assert result['partition'] == 0
        assert result['offset'] == 100
        assert result['data'] == {"temperature": 25.5}

    def test_parse_message_null(self, consumer):
        """Test parsing null message."""
        mock_message = Mock()
        mock_message.topic.return_value = "sensor-data-test"
        mock_message.value.return_value = None
        
        result = consumer.parse_message(mock_message)
        
        assert result is None

    def test_parse_message_invalid_json(self, consumer):
        """Test parsing invalid JSON message."""
        mock_message = Mock()
        mock_message.topic.return_value = "sensor-data-test"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 100
        mock_message.value.return_value = b"invalid json"
        mock_message.timestamp.return_value = (1, 1704067200000)
        
        result = consumer.parse_message(mock_message)
        
        assert result is not None
        assert result['data'] == {"value": "invalid json"}

    def test_get_metrics(self, consumer):
        """Test getting consumer metrics."""
        consumer.messages_consumed = 100
        consumer.messages_failed = 5
        consumer.last_message_time = datetime(2024, 1, 1, 12, 0, 0)
        consumer.running = True
        
        metrics = consumer.get_metrics()
        
        assert metrics['messages_consumed'] == 100
        assert metrics['messages_failed'] == 5
        assert metrics['is_running'] is True
        assert 'last_message_time' in metrics

    def test_health_check_healthy(self, consumer):
        """Test health check when healthy."""
        consumer.consumer = Mock()
        consumer.last_message_time = datetime.utcnow()
        
        health = consumer.health_check()
        
        assert health['healthy'] is True
        assert len(health['issues']) == 0

    def test_health_check_not_connected(self, consumer):
        """Test health check when not connected."""
        consumer.consumer = None
        
        health = consumer.health_check()
        
        assert health['healthy'] is False
        assert "Not connected to Kafka" in health['issues']

    def test_health_check_no_recent_messages(self, consumer):
        """Test health check with no recent messages."""
        consumer.consumer = Mock()
        consumer.last_message_time = datetime(2024, 1, 1, 12, 0, 0)
        
        with patch('app.kafka.consumer.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2024, 1, 1, 12, 10, 0)
            health = consumer.health_check()
        
        assert len(health['issues']) > 0
        assert "No messages received" in health['issues'][0]

    @patch('app.kafka.consumer.Consumer')
    def test_start_consuming(self, mock_consumer_class, consumer):
        """Test message consumption."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_message = Mock()
        mock_message.error.return_value = None
        mock_message.topic.return_value = "sensor-data-test"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 100
        mock_message.value.return_value = json.dumps({"value": 42}).encode('utf-8')
        mock_message.timestamp.return_value = (1, 1704067200000)
        
        consumer.connect()
        handler = Mock()
        consumer.set_message_handler(handler)
        
        # Simulate consuming one message then stopping
        poll_count = 0
        def poll_side_effect(timeout):
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                return mock_message
            consumer.running = False
            return None
        
        mock_consumer.poll.side_effect = poll_side_effect
        consumer.start_consuming()
        
        assert consumer.messages_consumed == 1
        handler.assert_called_once()

    def test_stop_consuming(self, consumer):
        """Test stopping consumption."""
        mock_consumer = Mock()
        consumer.consumer = mock_consumer
        consumer.running = True
        
        consumer.stop_consuming()
        
        assert consumer.running is False
        assert consumer.consumer is None
        mock_consumer.close.assert_called_once()