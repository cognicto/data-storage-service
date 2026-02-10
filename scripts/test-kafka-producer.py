#!/usr/bin/env python3
"""
Test Kafka producer to generate sample sensor data.
"""

import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Kafka configuration
KAFKA_SERVERS = ['localhost:9092']
SENSOR_TOPICS = [
    'sensor-data-quad_ch1',
    'sensor-data-quad_ch2', 
    'sensor-data-quad_ch3',
    'sensor-data-quad_ch4',
    'sensor-data-quad_ch5',
    'sensor-data-quad_ch6',
    'sensor-data-quad_ch7',
    'sensor-data-quad_ch8',
    'sensor-data-quad_ch9',
    'sensor-data-quad_ch10',
    'sensor-data-quad_ch11',
    'sensor-data-quad_ch12',
    'sensor-data-quad_ch13',
    'sensor-data-quad_ch14'
]

def generate_sensor_data(sensor_name: str) -> dict:
    """Generate realistic sensor data."""
    return {
        "asset_id": f"asset_{random.randint(1, 5):03d}",
        "sensor_id": sensor_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2),
        "pressure": round(random.uniform(980.0, 1020.0), 2),
        "vibration_x": round(random.uniform(-2.0, 2.0), 3),
        "vibration_y": round(random.uniform(-2.0, 2.0), 3),
        "vibration_z": round(random.uniform(-2.0, 2.0), 3),
        "power_consumption": round(random.uniform(100.0, 500.0), 1),
        "status": random.choice(["normal", "warning", "normal", "normal"]),
        "metadata": {
            "location": f"Building_{random.randint(1, 3)}_Floor_{random.randint(1, 5)}",
            "firmware_version": "v1.2.3",
            "last_calibration": "2024-01-01T00:00:00Z"
        }
    }

def main():
    """Main producer function."""
    print("Starting Kafka test producer...")
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    message_count = 0
    
    try:
        while True:
            # Send data for each sensor
            for topic in SENSOR_TOPICS:
                sensor_name = topic.replace('sensor-data-', '')
                data = generate_sensor_data(sensor_name)
                
                # Send message
                future = producer.send(
                    topic,
                    key=sensor_name,
                    value=data
                )
                
                message_count += 1
                
                # Print progress
                if message_count % 50 == 0:
                    print(f"Sent {message_count} messages...")
            
            # Wait before next batch
            time.sleep(5)  # Send batch every 5 seconds
            
    except KeyboardInterrupt:
        print(f"\nStopping producer. Sent {message_count} total messages.")
    
    finally:
        producer.close()

if __name__ == "__main__":
    main()