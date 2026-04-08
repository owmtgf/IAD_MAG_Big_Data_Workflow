import time
import threading
from tqdm import tqdm
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import col, avg, count, sum as _sum, desc


def now():
    return time.perf_counter()


def get_container_memory_mb():
    try:
        with open("/sys/fs/cgroup/memory.current", "r") as f:
            return int(f.read()) / 1024 / 1024
    except FileNotFoundError:
        try:
            with open("/sys/fs/cgroup/memory/memory.usage_in_bytes", "r") as f:
                return int(f.read()) / 1024 / 1024
        except:
            return None
        

def monitor_container_memory(interval, stop_event, storage):
    while not stop_event.is_set():
        mem = get_container_memory_mb()
        if mem is not None:
            storage.append(mem)
        time.sleep(interval)


def run_with_memory_tracking(fn, interval=0.05):
    memory_trace = []
    stop_event = threading.Event()

    monitor_thread = threading.Thread(
        target=monitor_container_memory,
        args=(interval, stop_event, memory_trace)
    )

    monitor_thread.start()

    t0 = now()
    fn()
    t1 = now()

    stop_event.set()
    monitor_thread.join()

    return {
        "time": round(t1 - t0, 4),
        "memory_trace": memory_trace
    }


def run_multiple_times(fn, runs=20):
    times = []
    peak_memories = []
    deltas = []

    for _ in tqdm(range(3), desc="Warmup..."):
        fn()

    for _ in tqdm(range(runs), desc="Progress"):
        result = run_with_memory_tracking(fn)

        times.append(result["time"])

        trace = result["memory_trace"]

        if trace:
            baseline = trace[0]
            peak = max(trace)
            delta = peak - baseline
        else:
            baseline = peak = delta = 0

        peak_memories.append(peak)
        deltas.append(delta)

    return {
        "timings": times,
        "avg_time": round(sum(times) / len(times), 4),
        "min_time": min(times),
        "max_time": max(times),
        "peak_memory_mb": peak_memories,
        "avg_peak_memory_mb": round(sum(peak_memories) / len(peak_memories), 2),
        "memory_delta_mb": deltas,
        "avg_delta_mb": round(sum(deltas) / len(deltas), 2)
    }


def benchmark_query_light(df, metrics: dict, runs=10):
    scenarios = {
        "popularity_30": lambda: df.filter(col("popularity") > 30).count(),
        "popularity_50": lambda: df.filter(col("popularity") > 50).count(),
        "popularity_70": lambda: df.filter(col("popularity") > 70).count(),
    }

    metrics["run_stats"]["light"] = {}

    for name, fn in scenarios.items():
        print(f"[LIGHT] Running {name}")
        metrics["run_stats"]["light"][name] = run_multiple_times(fn, runs)


def benchmark_query_medium(df, metrics: dict, runs=10):
    scenarios = {
        "group_genre": lambda: df.groupBy("track_genre").agg(
            avg("energy"),
            avg("danceability"),
            count("*")
        ).persist().count(),

        "group_genre_filtered": lambda: df.filter(
            col("popularity") > 50
        ).groupBy("track_genre").agg(
            avg("energy"),
            count("*")
        ).collect(),
    }

    metrics["run_stats"]["medium"] = {}

    for name, fn in scenarios.items():
        print(f"[MEDIUM] Running {name}")
        metrics["run_stats"]["medium"][name] = run_multiple_times(fn, runs)


def benchmark_query_heavy(df, metrics: dict, runs=10):
    scenarios = {
        "self_join": lambda: df.alias("a").join(
            df.alias("b"),
            col("a.artist") == col("b.artist")
        ).limit(100000).collect(),

        "window_rank": lambda: df.withColumn(
            "rank",
            rank().over(
                Window.partitionBy("artist").orderBy(desc("popularity"))
            )
        ).collect(),

        "full_sort": lambda: df.orderBy(desc("popularity")).collect(),

        "cross_join_sample": lambda: df.limit(5000).crossJoin(
            df.limit(5000)
        ).count(),
    }

    metrics["run_stats"]["heavy"] = {}

    for name, fn in scenarios.items():
        print(f"[HEAVY] Running {name}")
        metrics["run_stats"]["heavy"][name] = run_multiple_times(fn, runs)
