import time
import json
import random
import pandas as pd
from pathlib import Path
from collections import deque
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


DATA_DIR = Path("/app/data_source")
FILES = sorted(list(DATA_DIR.iterdir()))
print("Files in data_source:", FILES)

POOL = deque()
BAD_FILES = set()


def refill_pool(pool, csv_files):
    """
    Clear the pool and refill it with rows from a randomly chosen CSV file.
    Skips files where any of the features {'resp_bytes', 'orig_bytes', 'duration'}
    have >50% missing values (missing = {"-", "unknown", None}) to keep overall data flow realistic.
    Files that fail are added to BAD_FILES and will be skipped in future calls.
    """
    if not csv_files:
        raise RuntimeError("No CSV files found.")

    good_files = [f for f in csv_files if f not in BAD_FILES]
    if not good_files:
        raise RuntimeError("All CSV files have been marked as bad (exceed 50% missingness).")

    files_to_try = good_files[:]
    random.shuffle(files_to_try)

    chosen = None
    df = None
    missing_vals = {"-", "unknown", None}
    cols_to_check = {'resp_bytes', 'orig_bytes', 'duration'}

    for candidate in files_to_try:
        print(f"Checking file: {candidate.name}")
        df = pd.read_csv(candidate)

        file_ok = True
        for col in cols_to_check:
            if col not in df.columns:
                print(f"Column {col} not found in {candidate.name}, marking file as bad.")
                BAD_FILES.add(candidate)
                file_ok = False
                break

            missing_mask = df[col].isin(missing_vals) | pd.isna(df[col])
            missing_prop = missing_mask.mean()

            if missing_prop > 0.5:
                print(f"File {candidate.name} has {missing_prop:.2%} missing in {col}, marking as bad.")
                BAD_FILES.add(candidate)
                file_ok = False
                break

        if file_ok:
            chosen = candidate
            break
    else:
        raise RuntimeError("No suitable CSV file found after checking all remaining files.")

    print(f"Refilling pool from {chosen.name}")
    if "label" in df.columns:
        df.drop(columns=["label"], inplace=True)

    pool.clear()
    for _, row in df.iterrows():
        pool.append(row.to_dict())

    print(f"Pool now has {len(pool)} rows")


producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka1:9092', 'kafka2:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka.")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying...")
        time.sleep(2)


while True:
    while not POOL:
        refill_pool(POOL, FILES)

    row_dict = POOL.popleft()

    producer.send('raw_data', row_dict)
    print(f"Sent: {row_dict}")

    time.sleep(random.uniform(0.5, 2.5))