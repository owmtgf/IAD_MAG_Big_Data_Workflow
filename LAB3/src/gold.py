import os
import polars as pl

from deltalake import DeltaTable
from deltalake.writer import write_deltalake


SOURCE = "../lakehouse/silver"

GOLD_AGG_AIRPORT = "../lakehouse/gold/agg_by_airport"
GOLD_AGG_AIRLINE = "../lakehouse/gold/agg_by_airline"
GOLD_AGG_HOUR = "../lakehouse/gold/agg_by_hour"
GOLD_AGG_SEASON = "../lakehouse/gold/agg_by_season"
GOLD_FEATURES = "../lakehouse/gold/features"


def write_delta(name, path, lazy_df, partition_by=None):
    df = lazy_df.collect()

    write_deltalake(
        path,
        df.to_arrow(),
        mode="overwrite",
        partition_by=partition_by,
    )

    dt = DeltaTable(path)

    print(f"{name}:")
    print(f"  rows: {df.height}")
    print(f"  columns: {len(df.columns)}")
    print(f"  delta_version: {dt.version()}")
    print(f"  files_count: {len(dt.files())}")

    return df


def main():
    print("=== GOLD LAYER START ===")

    if not os.path.exists(SOURCE):
        raise FileNotFoundError(f"Silver table not found: {SOURCE}")

    silver_dt = DeltaTable(SOURCE)

    print(f"Silver source version: {silver_dt.version()}")
    print(f"Silver source files: {len(silver_dt.files())}")

    lf = pl.scan_delta(SOURCE)

    input_rows = lf.select(pl.len()).collect().item()
    print(f"Input silver rows: {input_rows}")

    agg_by_airport = (
        lf
        .group_by("origin")
        .agg([
            pl.len().alias("flights_count"),
            pl.col("arr_delay").mean().alias("avg_arr_delay"),
            pl.col("dep_delay").mean().alias("avg_dep_delay"),
            pl.col("is_delayed").mean().alias("delay_rate"),
        ])
        .sort("avg_arr_delay", descending=True)
    )

    agg_by_airline = (
        lf
        .group_by("airline")
        .agg([
            pl.len().alias("flights_count"),
            pl.col("arr_delay").mean().alias("avg_arr_delay"),
            pl.col("dep_delay").mean().alias("avg_dep_delay"),
            pl.col("is_delayed").mean().alias("delay_rate"),
        ])
        .sort("avg_arr_delay", descending=True)
    )

    agg_by_hour = (
        lf
        .group_by("hour")
        .agg([
            pl.len().alias("flights_count"),
            pl.col("arr_delay").mean().alias("avg_arr_delay"),
            pl.col("is_delayed").mean().alias("delay_rate"),
        ])
        .sort("hour")
    )

    agg_by_season = (
        lf
        .group_by("season")
        .agg([
            pl.len().alias("flights_count"),
            pl.col("arr_delay").mean().alias("avg_arr_delay"),
            pl.col("is_delayed").mean().alias("delay_rate"),
        ])
        .sort("season")
    )

    features = (
        lf
        .select([
            "flight_id",
            "year",
            "month",
            "flight_date",
            "origin",
            "dest",
            "route",
            "airline",
            "distance",
            "hour",
            "day_of_week",
            "season",
            "dep_delay",
            "arr_delay",
            "is_delayed",
        ])
    )

    print("=== GOLD LAZY QUERY PLAN EXAMPLE ===")
    print(agg_by_airport.explain())

    print("=== WRITING GOLD TABLES ===")

    airport_df = write_delta("agg_by_airport", GOLD_AGG_AIRPORT, agg_by_airport)
    airline_df = write_delta("agg_by_airline", GOLD_AGG_AIRLINE, agg_by_airline)
    hour_df = write_delta("agg_by_hour", GOLD_AGG_HOUR, agg_by_hour)
    season_df = write_delta("agg_by_season", GOLD_AGG_SEASON, agg_by_season)

    features_df = write_delta(
        "features",
        GOLD_FEATURES,
        features,
        partition_by=["year", "month"],
    )

    print("=== GOLD TABLE PREVIEWS ===")
    print("Top 5 delayed origin airports:")
    print(airport_df.head(5))

    print("Airline aggregates:")
    print(airline_df)

    print("Hourly aggregates:")
    print(hour_df.head(24))

    print("Season aggregates:")
    print(season_df)

    print("=== GOLD FEATURE TABLE INFO ===")
    gold_features_dt = DeltaTable(GOLD_FEATURES)
    print(f"Gold features version: {gold_features_dt.version()}")
    print(f"Gold features rows: {features_df.height}")
    print(f"Gold features partitions: ['year', 'month']")

    print("=== GOLD LAYER FINISHED ===")


if __name__ == "__main__":
    main()
    