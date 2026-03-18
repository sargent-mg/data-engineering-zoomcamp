from kafka import KafkaConsumer
import json

# Initialize Consumer
consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='homework-check-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000
)

print("Consumer started... waiting for data.")

count = 0
try:
    for message in consumer:
        if message.value.get('trip_distance', 0) > 5.0:
            count += 1
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()

print(f"Total trips with distance > 5.0: {count}")