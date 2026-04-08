from pyspark.sql import SparkSession


def init_spark():
    spark = (
        SparkSession.builder
        .appName("Spotify Benchmark Lab")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
