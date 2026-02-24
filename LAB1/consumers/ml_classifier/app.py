import json
import time
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable


INPUT_TOPIC = "processed_data"
OUTPUT_TOPIC = "predictions"
DATA_QUALITY_TOPIC = "data_quality"

BOOTSTRAP_SERVERS = ['kafka3:9092', 'kafka4:9092']

MODEL_PATH = "/app/model/xgb_model.json"
FEATURES_PATH = "/app/model/feature_cols.json"


print("Loading model...")

model = xgb.Booster()
model.load_model(MODEL_PATH)

with open(FEATURES_PATH) as f:
    FEATURE_COLS = json.load(f)

print("Model + feature schema loaded.")


consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="ml-consumer-group"
        )
        print("Connected to Kafka (consumer).")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying consumer...")
        time.sleep(2)

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka (producer).")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying producer...")
        time.sleep(2)


print("Starting ML inference loop...")

for message in consumer:
    try:
        input_dict = message.value

        df = pd.DataFrame([input_dict])

        missing = set(FEATURE_COLS) - set(df.columns)
        if missing:
            producer.send(DATA_QUALITY_TOPIC, {
                "type": "ml_missing_features",
                "missing_features": list(missing),
                "original_message": input_dict
            })
            print(f"Missing features for ML: {missing}")
            continue

        X = df[FEATURE_COLS].values.astype(float)

        dmatrix = xgb.DMatrix(X)

        preds = model.predict(dmatrix)

        pred_class = int(np.argmax(preds, axis=1)[0])

        output_event = {
            "prediction": pred_class,
            "probabilities": preds.tolist()[0]
        }

        producer.send(OUTPUT_TOPIC, output_event)

        print(f"Prediction sent: {output_event}")

    except Exception as e:
        producer.send(DATA_QUALITY_TOPIC, {
            "type": "ml_exception",
            "error": str(e),
            "original_message": input_dict
        })
        print(f"ML processing error: {e}")
        