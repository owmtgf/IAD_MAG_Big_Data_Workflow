import time

def now():
    return time.perf_counter()

def load_data(spark, path, metrics):
    t = now()
    df = spark.read.parquet(path)

    metrics["partitions"] = df.rdd.getNumPartitions()
    df.explain(True)

    metrics["timings"]["read"] = round(now() - t, 4)
    return df
