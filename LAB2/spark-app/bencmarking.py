import time
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
from pyspark.sql.functions import col, avg, count, sum as _sum, desc

from app import SPARK


def now():
    return time.perf_counter()

def parse_memory_status(spark: SparkSession):
    mem = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()

    result = []
    iterator = mem.iterator()
    while iterator.hasNext():
        entry = iterator.next()
        executor = entry._1()
        values = entry._2()
        total = values._1()
        free = values._2()
        used = total - free

        result.append({
            "executor": executor,
            "total_mb": round(total / 1024 / 1024, 2),
            "used_mb": round(used / 1024 / 1024, 2),
            "free_mb": round(free / 1024 / 1024, 2),
        })

    return result


def run_multiple_times(fn, runs=10):
    times = []
    memories = []

    # warmup
    for _ in range(3):
        fn()

    for _ in range(runs):
        t0 = now()
        fn()
        t1 = now()

        times.append(round(t1 - t0, 4))
        memories.append(parse_memory_status(SPARK))

    return {
        "timings": times,
        "avg_time": round(sum(times) / len(times), 4),
        "min_time": min(times),
        "max_time": max(times),
        "memory_samples": memories
    }



def benchmark_query_light(df: DataFrame, metrics: dict, runs=10):
    scenarios = {
        "popularity_30": lambda: df.filter(col("popularity") > 30).count(),
        "popularity_50": lambda: df.filter(col("popularity") > 50).count(),
        "popularity_70": lambda: df.filter(col("popularity") > 70).count(),
    }

    metrics["run_stats"]["light"] = {}

    for name, fn in scenarios.items():
        print(f"[LIGHT] Running {name}")
        metrics["run_stats"]["light"][name] = run_multiple_times(fn, runs)


def benchmark_query_medium(df: DataFrame, metrics: dict, runs=10):
    scenarios = {
        "group_genre": lambda: df.groupBy("track_genre").agg(
            avg("energy"),
            avg("danceability"),
            count("*")
        ).count(),

        "group_genre_filtered": lambda: df.filter(
            col("popularity") > 50
        ).groupBy("track_genre").agg(
            avg("energy"),
            count("*")
        ).count(),
    }

    metrics["run_stats"]["medium"] = {}

    for name, fn in scenarios.items():
        print(f"[MEDIUM] Running {name}")
        metrics["run_stats"]["medium"][name] = run_multiple_times(fn, runs)


def benchmark_query_heavy(df: DataFrame, metrics: dict, runs=10):
    scenarios = {
        "artist_full": lambda: df.groupBy("artist").agg(
            avg("popularity"),
            _sum("duration_ms"),
            count("*")
        ).orderBy(desc("avg(popularity)")).count(),

        "artist_top100": lambda: df.groupBy("artist").agg(
            avg("popularity"),
            count("*")
        ).orderBy(desc("avg(popularity)")).limit(100).count(),

        "artist_filtered": lambda: df.filter(
            col("popularity") > 60
        ).groupBy("artist").agg(
            avg("popularity"),
            count("*")
        ).count(),
    }

    metrics["run_stats"]["heavy"] = {}

    for name, fn in scenarios.items():
        print(f"[HEAVY] Running {name}")
        metrics["run_stats"]["heavy"][name] = run_multiple_times(fn, runs)

