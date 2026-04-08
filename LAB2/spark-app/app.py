import time
import argparse

from utils.spark import init_spark
from utils.data import load_data
from utils.metrics import init_metrics, save_metrics
from utils.benchmarks import (
    benchmark_query_light,
    benchmark_query_medium,
    benchmark_query_heavy,
)


DATA_PATH = "hdfs://namenode:9000/input/spotify_tracks_clean.parquet"
LOG_DIR = "/logs"


def build_log_filename(nodes, optimized):
    opt = "opt" if optimized else "base"
    return f"{LOG_DIR}/run_nodes{nodes}_{opt}.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimized", action="store_true")
    parser.add_argument("--nodes", type=int, default=1)
    args = parser.parse_args()

    spark = init_spark()
    metrics = init_metrics(args)

    total_start = time.perf_counter()

    df = load_data(spark, DATA_PATH, metrics)

    if args.optimized:
        df = df.repartition(4).cache()

    benchmark_query_light(df, metrics)
    benchmark_query_medium(df, metrics)
    benchmark_query_heavy(df, metrics)

    metrics["total_time_sec"] = round(time.perf_counter() - total_start, 4)

    filename = build_log_filename(args.nodes, args.optimized)
    save_metrics(metrics, filename)

    spark.stop()

    print("Saved metrics to:", filename)


if __name__ == "__main__":
    main()
