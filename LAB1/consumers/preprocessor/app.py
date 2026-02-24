import json
import time
import joblib

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from preprocessing_scripts import run_through_pipeline


# define relative data topics
INPUT_TOPIC = 'raw_data'
OUTPUT_TOPIC = 'processed_data'
QUALITY_TOPIC = "data_quality"

# define relative bootstrap servers
BOOTSTRAP_SERVERS_IN = ['kafka1:9092', 'kafka2:9092']
BOOTSTRAP_SERVERS_OUT = ['kafka3:9092', 'kafka4:9092']

# initialize handlers
CONSUMER = None
PRODUCER = None

# initialize pre-trained scaler for processing
SCALER = joblib.load("/app/model/scaler.pkl")
 
# some global variables
MINOR_FEATURES = {
    "ts",
    "uid",
    "local_orig",
    "local_resp",
    "missed_bytes",
    "tunnel_parents",
    "service",
    "detailed-label",
}

COLUMNS_TO_SCALE = [
    "duration_log",
    "orig_pkts_log",
    "resp_pkts_log",
    "orig_bytes_log",
    "resp_bytes_log",
    "orig_ip_bytes_log",
    "resp_ip_bytes_log"
]

PLACEHOLDERS = {"-", "unknown", None}


def detect_missing_features(df: pd.DataFrame) -> list:
    missing_features = []

    for col in df.columns:
        val = df.iloc[0][col]

        if pd.isna(val) or val in PLACEHOLDERS:
            missing_features.append(col)

    return missing_features


while CONSUMER is None:
    try:
        CONSUMER = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS_IN,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='data-processor-group'
        )
        print("Connected to Kafka (consumer).")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying consumer...")
        time.sleep(2)


while PRODUCER is None:
    try:
        PRODUCER = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS_OUT,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka (producer).")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying producer...")
        time.sleep(2)


print("Starting processing loop...")

for message in CONSUMER:
    try:
        input_dict = message.value
        df = pd.DataFrame([input_dict])

        missing_features = []

        for col in df.columns:
            val = df.iloc[0][col]
            if pd.isna(val) or val in PLACEHOLDERS:
                missing_features.append(col)
        
        missing_features = set(missing_features)
        if missing_features.issubset(MINOR_FEATURES):
            missing_features = set()
        else:
            missing_features = missing_features - MINOR_FEATURES

        if missing_features:
            quality_event = {
                "type": "missing_values",
                "missing_features": list(missing_features),
                "original_message": input_dict
            }

            PRODUCER.send(QUALITY_TOPIC, quality_event)
            print(f"Data quality issue detected: {missing_features}")

            continue

        processed_df = run_through_pipeline(df, inference_mode=True)

        if processed_df is None or len(processed_df) == 0:
            print("Pipeline produced empty dataframe, skipping...")
            continue

        missing = set(COLUMNS_TO_SCALE) - set(processed_df.columns)
        if missing:
            PRODUCER.send(QUALITY_TOPIC, {
                "type": "missing_columns_after_pipeline",
                "missing_columns": list(missing),
                "original_message": input_dict
            })
            print(f"Missing columns after pipeline: {missing}")
            continue

        scaled_values = SCALER.transform(processed_df[COLUMNS_TO_SCALE])
        processed_df[COLUMNS_TO_SCALE] = scaled_values

        output_dict = processed_df.iloc[0].to_dict()

        PRODUCER.send(OUTPUT_TOPIC, output_dict)
        print(f"Processed and sent: {output_dict}")

    except Exception as e:
        PRODUCER.send(QUALITY_TOPIC, {
            "type": "processing_exception",
            "error": str(e),
            "original_message": input_dict
        })
        print(f"Error processing message: {e}")
