# Spark + Hadoop Benchmark Project

This project demonstrates performance benchmarking of distributed data processing using Apache Spark in a containerized environment. It evaluates how different configurations (number of nodes and optimization strategies) affect execution time and memory usage.

---

## Dataset
Spotify Tracks Dataset (114k rows, 20 features)
## 📎 Dataset

The [dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) used in this project is based on Spotify tracks and includes features such as:

* track popularity
* energy
* danceability
* genre
* ...

There is some preprocessing was performed, you can find it in `./data/preprocessing.ipynb` file.


## 📁 Project Structure

* `./data/` \
  Contains the dataset, preprocessing notebook, and HDFS upload script.

* `./docker/` \
  Contains Docker setup and scripts to run the cluster and benchmarks.

  * `docker-compose.yml` - defines the Spark + HDFS cluster
  * `run_benchmark.sh` - runs a single benchmark
  * `run_all_benchmarks.sh` - runs full experiment suite

* `./results/` \
  Stores benchmark outputs and analysis:

  * JSON metrics from experiments
  * Logs from runs
  * Jupyter notebook with plots

* `./spark-app/` \
  Main Spark application:

  * `app.py` - entry point for benchmarks
  * `utils/` - helper modules for data processing, Spark setup, benchmarking and metrics

---

## 🚀 How to Run

### 1. Start the cluster and run benchmarks

```
cd ./docker/
bash run_all_benchmarks.sh
```

You can control the experiment using environment variables:

* `NUM_DATA_NODES` - number of datanodes (e.g., 1 or 3)
* `OPTIMIZED` - enable optimized execution (true/false)

---

### 2. Run an individual benchmark

```
cd ./docker/
bash run_benchmark.sh
```

---

### 3. View results

After execution, results are saved in:

* `results/run_nodes*.json` - raw metrics
* `results/logs/` - execution logs
* `results/plots.ipynb` - visualizations

---

## 📊 Benchmark Design

The project evaluates three types of queries:

* **Light** - simple filters and counts
* **Medium** - aggregations and grouping
* **Heavy** - joins, window functions, and sorting

Each query is executed multiple times, and the following metrics are collected:

* Execution time
* Memory consumption (delta)
* Speedup (for multi-node setups)

---

## ⚙️ Optimization

The project includes an optional optimization mode:

* Caching intermediate results
* Repartitioning data
* Reducing unnecessary recomputation

This can be enabled via the `--optimized` flag.

---

## 📈 Key Findings

* Increasing the number of nodes does not always lead to proportional performance gains.
* The benefit of distributed execution depends heavily on query complexity.
* Optimization improves performance for some queries, but not uniformly across all workloads.
* Memory usage is more stable when analyzed using delta rather than peak values.

---

## 📌 Notes

* Results may vary depending on hardware and container resource limits
* JVM garbage collection and caching can affect memory measurements
* First runs may be slower, so warm-up was utilized

---
