# Real-Time Network Malware Detection with Kafka

This project implements an end-to-end real‑time streaming pipeline for network malware detection. It uses Apache Kafka as the messaging backbone, multiple consumer groups for preprocessing, classification, and monitoring, and a Streamlit dashboard for live visualisation. The machine learning model (XGBoost) is trained offline on the [Network Malware Detection Connection Analysis dataset](https://www.kaggle.com/datasets/agungpambudi/network-malware-detection-connection-analysis) and then deployed inside a Docker container to score incoming data streams.

---

## Dataset

The dataset originates from Stratosphere Labs and contains network flow records labelled with seven classes:
- **0** - Normal (benign)
- **1** - Malicious Undefined
- **2** - Malicious C&C
- **3** - Malicious Attack
- **4** - Malicious File Download
- **5** - Malicious PartOfAHorizontalPortScan
- **6** - Malicious DDoS

The full dataset is extremely large (~25 million rows). To avoid memory issues, the preparation script processes it in chunks.

---

## Project Structure

```
.
├── README.md
├── classifier_preparation
│   ├── __pycache__
│   │   └── preprocessing_scripts.cpython-310.pyc
│   ├── data_preparation.ipynb
│   ├── dataset
│   ├── dataset_chunked_processed
│   ├── dataset_chunked_raw
│   ├── dataset_split
│   │   ├── producer
│   │   ├── test
│   │   └── train
│   ├── model
│   │   ├── feature_cols.json
│   │   ├── fit_log.txt
│   │   ├── scaler.pkl
│   │   └── xgb_model.json
│   ├── preprocessing_scripts.py
│   └── train_model.py
├── consumers
│   ├── dashboard
│   │   ├── Dockerfile
│   │   ├── app.py
│   │   └── requirements.txt
│   ├── ml_classifier
│   │   ├── Dockerfile
│   │   ├── app.py
│   │   └── requirements.txt
│   └── preprocessor
│       ├── Dockerfile
│       ├── app.py
│       └── requirements.txt
├── docker-compose.yml
├── producer
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
├── requirements.txt
└── run_containers.sh
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Miniconda](https://www.anaconda.com/docs/getting-started/miniconda/install) (for model preparation, optional if you prefer another Python environment)
- At least 16 GB RAM recommended (the dataset is large)
- Linux OS / WSL2

---

## Setup

### 1. Create a Conda environment and install dependencies

```bash
conda create -n malware-detection python=3.10
conda activate malware-detection
pip install -r requirements.txt
```

### 2. Prepare the data and train the model

Run the Jupyter notebook to download, chunk, and pre‑process the dataset:

```bash
jupyter notebook classifier_preparation/data_preparation.ipynb
```

This notebook will:
- Download the dataset (if not already present) into `classifier_preparation/dataset/`
- Split the raw data into smaller chunks (`dataset_chunked_raw/`)
- Process each chunk (handle missing values, encode categoricals, etc.) and save to `dataset_chunked_processed/`
- Create train/test/producer splits (`dataset_split/`)

After the notebook finishes, train the XGBoost model:

```bash
python classifier_preparation/train_model.py
```

This script loads the training data, scales features using `sklearn.preprocessing.StandardScaler`, trains an XGBoost classifier, and saves:
- `model/feature_cols.json` - ordered list of feature names expected by the model
- `model/scaler.pkl` - fitted scaler object
- `model/xgb_model.json` - trained XGBoost model in JSON format


## Running the Pipeline

### 1. Start all containers and create topics

Make the script executable and run it:

```bash
chmod +x run_containers.sh
./run_containers.sh
```

The script performs:

- `docker compose up -d --build` – builds and starts all services (Zookeeper, 4 Kafka brokers, 2 producers, preprocessor, classifier, dashboard)
- A 15‑second wait to allow Kafka brokers to initialise
- Manual creation of four topics (`raw_data`, `processed_data`, `data_quality`, `predictions`) with 8 partitions and replication factor 2
- Verification by listing the topics

### 2. Monitor the logs (optional)
You can follow the logs of individual containers:

```bash
docker logs -f producer1
docker logs -f data_preprocessor
docker logs -f ml_classifier
docker logs -f dashboard
```

### 3. Access the Streamlit dashboard

Once all containers are running, open your browser at http://localhost:8501, be patient, it can take a couple minutes untill the first dashboard update. The dashboard auto‑refreshes every 2 seconds and displays:

- Data Quality Monitoring
    - ML exceptions count
    - Missing features (pre‑pipeline) – total and recent bar charts
    - Missing columns after pipeline – total and recent bar charts
- Predictions Monitoring
    - Total predictions per label (bar chart)
    - Normal requests over the last 60 seconds (line chart)
    - Recent malware occurrences (labels 1‑6) – bar chart
- Model Confidence
    - Rolling confidence values (line chart)
    - Average confidence over the last 1000 predictions
- Uncertain Predictions – last 5 predictions with confidence < 0.5


## Pipeline Architecture
```
                    +-----------------+       +------------------+
                    |   Producer 1    |       |   Producer 2     |
                    | (reads CSV rows)|       | (reads CSV rows) |
                    +-------+---------+       +--------+---------+
                            |                           |
                            |        raw_data topic     |
                            +------------+--------------+
                                         |
                                         v
                            +-------------+-------------+
                            |    Preprocessor Consumer  |
                            | (cleaning, scaling, etc.) |
                            +-------------+-------------+
                                         |
                                processed_data topic
                                         |
                    +--------------------+--------------------+
                    |                                         |
                    v                                         v
      +-------------+-------------+               +-----------+-----------+
      |    ML Classifier Consumer |               | Dashboard Consumer    |
      |   (XGBoost inference)     |               | (metrics collection)  |
      +-------------+-------------+               +-----------------------+  
                    |                                         ^
                    | predictions topic                       |
                    +-----------------------------------------+
```

**Topics:**

- `raw_data`: JSON messages, each a single row from a CSV file (without the `label` column).
- `processed_data`: Same structure after preprocessing (features scaled, ready for inference).
- `data_quality`: Quality events (missing values, processing exceptions) emitted by the preprocessor and classifier.
- `predictions`: Model outputs containing `prediction` (integer label) and `probabilities` (list of 7 class probabilities).

**Services:**

- Producers (`producer1`, `producer2`): Read random CSV files from dataset, fill an internal pool, and send rows to `raw_data`. They skip files where any of `{'resp_bytes', 'orig_bytes', 'duration'}` have >50% missing values.
- Preprocessor Consumer: Listens to `raw_data`, applies the same preprocessing steps used during training (using `preprocessing_scripts.py` and the saved scaler), and forwards cleaned data to `processed_data`. It also sends data quality events (missing values, missing columns after pipeline) to `data_quality`.
- ML Classifier Consumer: Listens to `processed_data`, loads the pre‑trained XGBoost model, performs inference, and publishes results to `predictions`. It also emits ML exceptions (e.g., model loading errors) to `data_quality`.
- Dashboard Consumer: Listens to `data_quality` and `predictions`, aggregates metrics in Streamlit’s session state, and renders the live dashboard.


## Testing Resilience: Simulating a Broker Failure

To verify that your Kafka cluster tolerates broker failures and that replication works as expected, you can intentionally stop one broker while the pipeline is running.

Stop one Kafka broker (for example, `kafka1`):
```bash
docker stop kafka1
```

Observe the behaviour of a consumer, for instance `consumer1` (`preprocessor`):
```bash
docker compose logs -f consumer1
```

If the logs continue to show normal message processing (e.g., "Received:", "Processed and sent:") without interruption, replication is working correctly. The cluster automatically elects new leaders for the partitions that were on the failed broker, and consumers seamlessly switch to them.

Restart the broker to bring it back into the cluster:
```bash
docker start kafka1
```
After a short while, it will rejoin as an in‑sync replica.

## Stopping the Pipeline
To stop all containers and clean up the resources, run:
```bash
docker compose down
```

If you only want to pause the containers (without removing them), you can use:
```bash
docker compose stop
```
