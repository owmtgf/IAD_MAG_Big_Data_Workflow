import json
import time
from collections import deque, Counter
import numpy as np
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

# ---------- Configuration ----------
BOOTSTRAP_SERVERS = ['kafka1:9092', 'kafka2:9092', 'kafka3:9092', 'kafka4:9092']
DATA_QUALITY_TOPIC = "data_quality"
PREDICTIONS_TOPIC = "predictions"
REFRESH_MS = 2000
ROLLING_WINDOW_SIZE = 1000
TIME_WINDOW_SEC = 60

LABEL_MAP = {
    0: "Normal",
    1: "Malicious Undefined",
    2: "Malicious C&C",
    3: "Malicious Attack",
    4: "Malicious File Download",
    5: "Malicious PartOfAHorizontalPortScan",
    6: "Malicious DDoS"
}

st_autorefresh(interval=REFRESH_MS, key="refresh")
st.title("Real-Time ML Monitoring Dashboard")

def init_state():
    defaults = {
        "ml_exceptions": 0,
        "missing_values_total": {},          # cumulative counts per feature
        "missing_columns_total": {},         # cumulative counts per column
        # Rolling deques for recent events (store feature names)
        "missing_values_rolling": deque(maxlen=ROLLING_WINDOW_SIZE),
        "missing_columns_rolling": deque(maxlen=ROLLING_WINDOW_SIZE),
        # Predictions
        "pred_counts_total": {},              # cumulative counts per label
        "rolling_predictions": deque(maxlen=ROLLING_WINDOW_SIZE),  # (timestamp, label)
        "rolling_confidence": deque(maxlen=ROLLING_WINDOW_SIZE),
        "uncertain": deque(maxlen=5)          # last 5 uncertain events
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_state()

@st.cache_resource
def create_consumers():
    dq = KafkaConsumer(
        DATA_QUALITY_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    preds = KafkaConsumer(
        PREDICTIONS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    return dq, preds

dq_consumer, pred_consumer = create_consumers()

# data quality messages
dq_msgs = dq_consumer.poll(timeout_ms=50)
for _, messages in dq_msgs.items():
    for msg in messages:
        event = msg.value
        t = event.get("type")

        if t == "ml_exception":
            st.session_state.ml_exceptions += 1

        elif t == "missing_values":
            for feat in event["missing_features"]:
                st.session_state.missing_values_total[feat] = \
                    st.session_state.missing_values_total.get(feat, 0) + 1
            st.session_state.missing_values_rolling.extend(event["missing_features"])

        elif t == "missing_columns_after_pipeline":
            for col in event["missing_columns"]:
                st.session_state.missing_columns_total[col] = \
                    st.session_state.missing_columns_total.get(col, 0) + 1
            st.session_state.missing_columns_rolling.extend(event["missing_columns"])

# prediction messages
pred_msgs = pred_consumer.poll(timeout_ms=50)
for _, messages in pred_msgs.items():
    for msg in messages:
        event = msg.value
        label = event["prediction"]
        probs = np.array(event["probabilities"])
        confidence = float(np.max(probs))
        timestamp = time.time()

        st.session_state.pred_counts_total[label] = \
            st.session_state.pred_counts_total.get(label, 0) + 1

        st.session_state.rolling_predictions.append((timestamp, label))

        st.session_state.rolling_confidence.append(confidence)

        if confidence < 0.5:
            st.session_state.uncertain.append(event)

def recent_counts(deque_of_items):
    """Return a Series of counts for the last N items."""
    return pd.Series(deque_of_items).value_counts()

st.header("Data Quality Monitoring")

# Row 1: ML exceptions and missing features (total + recent)
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("ML Exceptions", st.session_state.ml_exceptions)

with col2:
    st.subheader("Missing Features (total)")
    if st.session_state.missing_values_total:
        df_total_missing = pd.DataFrame.from_dict(
            st.session_state.missing_values_total,
            orient="index",
            columns=["count"]
        ).sort_index()
        st.bar_chart(df_total_missing)
    else:
        st.info("No missing features recorded.")

with col3:
    st.subheader("Missing Features (recent)")
    if st.session_state.missing_values_rolling:
        recent_missing = recent_counts(st.session_state.missing_values_rolling)
        df_recent_missing = recent_missing.to_frame(name="count").sort_index()
        st.bar_chart(df_recent_missing)
    else:
        st.info("No recent missing features.")

# Row 2: Missing columns after pipeline (total + recent)
col1, col2 = st.columns(2)
with col1:
    st.subheader("Missing Columns After Pipeline (total)")
    if st.session_state.missing_columns_total:
        df_total_cols = pd.DataFrame.from_dict(
            st.session_state.missing_columns_total,
            orient="index",
            columns=["count"]
        ).sort_index()
        st.bar_chart(df_total_cols)
    else:
        st.info("No missing columns recorded.")

with col2:
    st.subheader("Missing Columns After Pipeline (recent)")
    if st.session_state.missing_columns_rolling:
        recent_cols = recent_counts(st.session_state.missing_columns_rolling)
        df_recent_cols = recent_cols.to_frame(name="count").sort_index()
        st.bar_chart(df_recent_cols)
    else:
        st.info("No recent missing columns.")

st.header("Predictions Monitoring")

# Row 1: Total counts per label
if st.session_state.pred_counts_total:
    df_preds_total = pd.DataFrame([
        {"label": LABEL_MAP.get(k, k), "count": v}
        for k, v in st.session_state.pred_counts_total.items()
    ]).set_index("label").sort_index()
    st.subheader("Total Predictions per Label")
    st.bar_chart(df_preds_total)

# Row 2: Line chart of normal requests over time
if st.session_state.rolling_predictions:
    df_rolling = pd.DataFrame(
        st.session_state.rolling_predictions,
        columns=["timestamp", "label"]
    )
    df_normal = df_rolling[df_rolling["label"] == 0].copy()
    if not df_normal.empty:
        df_normal["datetime"] = pd.to_datetime(df_normal["timestamp"], unit="s")
        df_normal.set_index("datetime", inplace=True)
        now = time.time()
        start_time = now - TIME_WINDOW_SEC
        df_normal = df_normal[df_normal["timestamp"] >= start_time]
        if not df_normal.empty:
            normal_rate = df_normal.resample("1S").size()
            st.subheader("Normal Requests (last 60 seconds)")
            st.line_chart(normal_rate)
        else:
            st.info("No normal requests in the last 60 seconds.")
    else:
        st.info("No normal requests recorded.")

if st.session_state.rolling_predictions:
    df_rolling = pd.DataFrame(
        st.session_state.rolling_predictions,
        columns=["timestamp", "label"]
    )
    malware_recent = df_rolling[df_rolling["label"].between(1, 6)]["label"].value_counts()
    if not malware_recent.empty:
        malware_recent.index = malware_recent.index.map(lambda x: LABEL_MAP.get(x, f"Label {x}"))
        st.subheader("Recent Malware Occurrences (last 1000 predictions)")
        st.bar_chart(malware_recent)
    else:
        st.info("No recent malware predictions.")

st.header("Model Confidence")

if st.session_state.rolling_confidence:
    conf_series = pd.Series(list(st.session_state.rolling_confidence)[-100:])
    st.line_chart(conf_series)

    avg_conf = np.mean(st.session_state.rolling_confidence)
    st.metric("Average Confidence (last 1000 predictions)", f"{avg_conf:.3f}")
else:
    st.info("No confidence data yet.")

st.header("Uncertain Predictions")

if st.session_state.uncertain:
    st.write(f"Last {len(st.session_state.uncertain)} uncertain predictions (confidence < 0.5):")
    st.json(list(st.session_state.uncertain))
else:
    st.info("No uncertain predictions recorded.")
    