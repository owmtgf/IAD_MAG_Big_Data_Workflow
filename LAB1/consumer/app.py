import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


consumer = None

while consumer is None:
    try:
        consumer = KafkaConsumer(
            'topic-1', 'topic-2',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='test-group'
        )
        print("Connected to Kafka.")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying...")
        time.sleep(2)

for message in consumer:
    print("Received:", message.value)