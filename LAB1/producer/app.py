import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka.")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying...")
        time.sleep(2)


while True:
    data_topic_1 = {
        "feature_1": random.random(),
        "feature_2": random.randint(0, 100),
        "feature_3": random.choice(["A", "B", "C"])
    }
    data_topic_2 = {
        "feature_4": random.random(),
        "feature_5": random.randint(0, 100),
        "feature_6": random.choice(["A", "B", "C"])
    }

    producer.send('topic-1', data_topic_1)
    producer.send('topic-2', data_topic_2)
    print("Chunk sent.")

    time.sleep(1)