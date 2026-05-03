# Flight Delay Lakehouse with Polars, Delta Lake and MLflow

## Overview

This project implements a local Lakehouse pipeline for US flight delay prediction using:

- Polars Lazy API
- Delta Lake
- MLflow
- Docker Compose
- scikit-learn

The pipeline follows the standard Lakehouse architecture:

```text
CSV dataset -> Bronze -> Silver -> Gold -> ML
```

The original dataset was named as a 2018-2024 flight delay dataset, but the downloaded file contains only January 2024 records. Because of that, the Bronze ingestion is implemented as daily incremental batches by `FlightDate` instead of yearly batches.

---

## Project Structure

```text
.
├── dataset/
│   └── flight_data_2018_2024.csv
├── lakehouse/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── logs/
│   ├── mlflow.db
│   └── mlruns/
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── train.py
│   ├── time_travel_demo.py
│   └── schema_evolution_demo.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── run_pipeline.sh
```

---

## How to Run

Run the full pipeline with:

```bash
bash run_pipeline.sh
```

The script starts MLflow through Docker Compose, waits until the server is ready, and then runs the application pipeline.

MLflow UI is available at:

```text
http://localhost:5000
```

The app container runs:

```text
bronze.py
silver.py
silver.py
gold.py
train.py
time_travel_demo.py
schema_evolution_demo.py
```

`silver.py` is executed twice intentionally: the first run creates the Silver table, and the second run demonstrates Delta Lake `MERGE`.

---

## Storage

The project uses local Delta Lake folders:

```text
lakehouse/bronze
lakehouse/silver
lakehouse/gold
```

MinIO/S3 was not used in this implementation because local storage was enough for the lab dataset size.

---

## Bronze Layer

### Goal

The Bronze layer stores raw CSV data in Delta format.

Since the dataset only contains January 2024, incremental ingestion was implemented by daily batches using `FlightDate`.

### Input Dataset

```text
Raw dataset shape: (582425, 120)
Raw columns count: 120
```

The source CSV contained one empty unnamed column, which was removed:

```text
Dropped empty unnamed column
Cleaned columns count: 119
```

### Incremental Loading

The dataset was loaded by 31 daily batches:

```text
Incremental batches by FlightDate: 31
First batch date: 2024-01-01
Last batch date: 2024-01-31
```

Each batch was written to the Bronze Delta table. The first batch used `overwrite`, and all following batches used `append`.

Example:

```text
[Bronze batch 01/31] FlightDate=2024-01-01, rows=18338, mode=overwrite, delta_version=0
[Bronze batch 02/31] FlightDate=2024-01-02, rows=20169, mode=append, delta_version=1
...
[Bronze batch 31/31] FlightDate=2024-01-31, rows=17786, mode=append, delta_version=30
```

Total loaded rows:

```text
Total loaded rows: 582425
```

### Delta OPTIMIZE

After ingestion, `OPTIMIZE compact` was applied to Bronze. This is useful because daily append created many small Parquet files.

Before optimization:

```text
Bronze current version: 30
```

Optimize result:

```text
numFilesAdded: 1
numFilesRemoved: 31
totalConsideredFiles: 31
```

After optimization:

```text
Bronze current version: 31
```

This shows that compaction created a new Delta version and compacted 31 small files into 1 larger file.

---

## Silver Layer

### Goal

The Silver layer cleans the raw Bronze data and creates features for analytics and ML.

### Transformations

The Silver layer performs:

- removal of cancelled flights;
- removal of rows with null `ArrDelay` or `DepDelay`;
- outlier filtering:
  - `arr_delay` between -60 and 300 minutes;
  - `dep_delay` between -60 and 300 minutes;
- category normalization:
  - `origin`;
  - `dest`;
  - `airline`;
- column subset selection;
- derived feature creation:
  - `hour`;
  - `day_of_week`;
  - `season`;
  - `route`;
  - `is_delayed`;
  - `flight_id`.

The Silver table is partitioned by:

```python
partition_by=["year", "month"]
```

### Partitioning Justification

Flight data is naturally time-based. Partitioning by `year` and `month` is a scalable choice for multi-year datasets because common analytical queries usually filter by time periods.

In this specific dataset, only January 2024 is available, so the final table contains one physical partition:

```text
year=2024/month=1
```

However, the partitioning strategy remains valid for larger multi-month or multi-year datasets.

### Silver Output

Input Bronze rows:

```text
Input bronze rows: 582425
```

Output Silver rows:

```text
Output silver rows: 554143
Output silver columns: 15
Rows removed: 28282
Rows kept ratio: 0.9514
```

Delay target summary:

```text
min_arr_delay: -60.0
avg_arr_delay: 6.666193
max_arr_delay: 300.0
delay_rate: 0.228073
```

Partition distribution:

```text
year: 2024
month: 1
rows: 554143
```

### Polars Lazy Query Plan

The Silver layer uses Polars Lazy API with `pl.scan_delta`.

The query plan shows projection and selection pushdown:

```text
Parquet SCAN [/app/lakehouse/bronze/part-00000-60f86c29-16d0-4308-b300-f7cbc7380fa0-c000.zstd.parquet]
PROJECT 12/119 COLUMNS
SELECTION: [([([(col("DepDelay").is_not_null()) & (col("DepDelay").is_between([dyn int: -60, dyn int: 300]))]) & ([(col("ArrDelay").is_not_null()) & (col("ArrDelay").is_between([dyn int: -60, dyn int: 300]))])]) & ([(col("Cancelled")) == (0.0)])]
```

This means Polars only reads 12 required columns out of 119 and applies filters at scan level.

### MERGE Demonstration

The Silver pipeline was run twice.

First run:

```text
Created silver table with partition_by=['year', 'month']
Silver current version: 0
```

Second run:

```text
Merged into existing silver table
MERGE result:
num_source_rows: 554143
num_target_rows_inserted: 0
num_target_rows_updated: 554143
num_output_rows: 554143
```

Delta version changed:

```text
Silver version before merge: 0
Silver current version: 1
```

This demonstrates that repeated pipeline execution updates existing records through `MERGE` instead of duplicating them.

---

## Gold Layer

### Goal

The Gold layer contains analytical data marts and the ML feature table.

### Gold Tables

The following Gold tables are created:

```text
lakehouse/gold/agg_by_airport
lakehouse/gold/agg_by_airline
lakehouse/gold/agg_by_hour
lakehouse/gold/agg_by_season
lakehouse/gold/features
```

### Gold Output

Input Silver rows:

```text
Input silver rows: 554143
```

Created tables:

```text
agg_by_airport: 351 rows, 5 columns
agg_by_airline: 10 rows, 5 columns
agg_by_hour: 24 rows, 4 columns
agg_by_season: 1 row, 4 columns
features: 554143 rows, 15 columns
```

The feature table is partitioned by:

```python
partition_by=["year", "month"]
```

### Gold Lazy Query Plan

The Gold layer also uses Polars Lazy API.

Example query plan for airport aggregation:

```text
AGGREGATE
  [len().alias("flights_count"), col("arr_delay").mean().alias("avg_arr_delay"), col("dep_delay").mean().alias("avg_dep_delay"), col("is_delayed").mean().alias("delay_rate")] BY [col("origin")]
  FROM
  Parquet SCAN [/app/lakehouse/silver/year=2024/month=1/part-00000-5750e0df-7f84-4a57-a367-1289e95b125a-c000.snappy.parquet]
  PROJECT 4/15 COLUMNS
```

This shows projection pushdown: only 4 required columns are read from the 15-column Silver table.

### Example Gold Results

Top delayed origin airports:

```text
ALO: avg_arr_delay=38.98, delay_rate=0.5306
MBS: avg_arr_delay=35.42, delay_rate=0.45
IMT: avg_arr_delay=35.18, delay_rate=0.3333
BRW: avg_arr_delay=34.85, delay_rate=0.4615
DLG: avg_arr_delay=34.66, delay_rate=0.4138
```

Airline aggregates:

```text
B6: avg_arr_delay=10.15, delay_rate=0.2778
AA: avg_arr_delay=10.12, delay_rate=0.2559
HA: avg_arr_delay=9.43, delay_rate=0.2547
AS: avg_arr_delay=8.57, delay_rate=0.2501
F9: avg_arr_delay=8.36, delay_rate=0.2571
```

Season aggregate:

```text
winter: flights_count=554143, avg_arr_delay=6.666193, delay_rate=0.228073
```

Only `winter` appears because the available dataset contains only January 2024.

---

## Machine Learning

### ML Tasks

Two ML tasks were implemented:

1. Regression:
   - target: `arr_delay`;
   - goal: predict arrival delay in minutes.

2. Classification:
   - target: `is_delayed`;
   - definition: `arr_delay > 15`.

### Features

The ML feature table includes:

```text
origin
dest
route
airline
distance
hour
day_of_week
month
dep_delay
```

Categorical features are encoded with `OneHotEncoder`.

### MLflow

MLflow is used to log:

- parameters;
- metrics;
- trained models;
- Gold feature table version.

Gold feature table version:

```text
Gold feature table version: 0
```

Training dataset:

```text
Dataset shape: (554143, 15)
Delay rate: 0.2281
```

### Model Results

Regression:

| Model | MAE | RMSE | R2 |
|---|---:|---:|---:|
| LinearRegression | 10.536 | 15.089 | 0.870 |
| RandomForestRegressor | 10.836 | 15.491 | 0.863 |

Classification:

| Model | Accuracy | F1 | ROC-AUC |
|---|---:|---:|---:|
| LogisticRegression | 0.919 | 0.803 | 0.931 |
| RandomForestClassifier | 0.909 | 0.797 | 0.878 |

### Model Comparison

For regression, `LinearRegression` slightly outperformed `RandomForestRegressor` on RMSE and R2.

For classification, `LogisticRegression` achieved the best F1 and ROC-AUC values.

---

## Delta Lake Features

This project demonstrates the following Delta Lake features.

### 1. MERGE

Used in the Silver layer to make repeated pipeline runs idempotent.

```text
num_target_rows_inserted: 0
num_target_rows_updated: 554143
```

### 2. OPTIMIZE / Compaction

Used in the Bronze layer after daily append ingestion.

```text
numFilesRemoved: 31
numFilesAdded: 1
```

### 3. VACUUM

VACUUM is used in dry-run mode as a safe maintenance operation.

```python
bronze_dt.vacuum(retention_hours=168, dry_run=True)
```

### 4. Time Travel

Time travel was demonstrated by reading Bronze version 0.

```text
Current version: 31
Old version (0) shape: (18338, 119)
```

Version 0 corresponds to the first daily batch, while the current Bronze version contains all January batches and the OPTIMIZE operation.

### 5. Schema Evolution

Schema evolution was demonstrated by adding a new column to the Silver table using `schema_mode="merge"`.

Before:

```text
Version: 1
Columns: ['flight_id', 'year', 'month', 'flight_date', 'origin', 'dest', 'route', 'airline', 'distance', 'hour', 'day_of_week', 'season', 'dep_delay', 'arr_delay', 'is_delayed']
```

After:

```text
Version: 2
Columns: ['flight_id', 'flight_date', 'origin', 'dest', 'route', 'airline', 'distance', 'hour', 'day_of_week', 'season', 'dep_delay', 'arr_delay', 'is_delayed', 'year', 'month', 'new_feature']
```

Preview:

```text
new_feature
1
1
1
1
1
```

---

## Notes and Limitations

- The downloaded dataset contains only January 2024, despite the original title mentioning 2018-2024.
- Because of this, incremental ingestion was implemented by daily batches instead of yearly batches.
- The `agg_by_season` Gold table contains only one season, `winter`.
- ML models were kept intentionally simple because the main goal of the lab is the Lakehouse pipeline, not maximum predictive accuracy.
