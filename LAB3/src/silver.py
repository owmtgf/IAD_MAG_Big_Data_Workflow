import os
import polars as pl

from deltalake import DeltaTable
from deltalake.writer import write_deltalake


SOURCE = "../lakehouse/bronze"
TARGET = "../lakehouse/silver"


def main():
    print("=== SILVER LAYER START ===")

    if not os.path.exists(SOURCE):
        raise FileNotFoundError(f"Bronze table not found: {SOURCE}")

    bronze_dt = DeltaTable(SOURCE)
    print(f"Bronze source version: {bronze_dt.version()}")

    lf = pl.scan_delta(SOURCE)

    raw_count = lf.select(pl.len()).collect().item()
    print(f"Input bronze rows: {raw_count}")

    silver = (
        lf
        .filter(pl.col("Cancelled") == 0)
        .filter(pl.col("ArrDelay").is_not_null())
        .filter(pl.col("DepDelay").is_not_null())
        .filter(pl.col("ArrDelay").is_between(-60, 300))
        .filter(pl.col("DepDelay").is_between(-60, 300))
        .with_columns([
            pl.col("Origin").str.strip_chars().str.to_uppercase().alias("origin"),
            pl.col("Dest").str.strip_chars().str.to_uppercase().alias("dest"),
            pl.col("IATA_Code_Marketing_Airline")
              .str.strip_chars()
              .str.to_uppercase()
              .alias("airline"),

            pl.col("Year").cast(pl.Int32).alias("year"),
            pl.col("Month").cast(pl.Int8).alias("month"),
            pl.col("FlightDate").cast(pl.Utf8).str.to_date().alias("flight_date"),

            (pl.col("CRSDepTime") // 100).cast(pl.Int8).alias("hour"),

            pl.col("FlightDate")
              .cast(pl.Utf8)
              .str.to_date()
              .dt.weekday()
              .cast(pl.Int8)
              .alias("day_of_week"),

            (
                pl.col("Origin").str.strip_chars().str.to_uppercase()
                + pl.lit("_")
                + pl.col("Dest").str.strip_chars().str.to_uppercase()
            ).alias("route"),

            pl.when(pl.col("Month").is_in([12, 1, 2]))
              .then(pl.lit("winter"))
              .when(pl.col("Month").is_in([3, 4, 5]))
              .then(pl.lit("spring"))
              .when(pl.col("Month").is_in([6, 7, 8]))
              .then(pl.lit("summer"))
              .otherwise(pl.lit("autumn"))
              .alias("season"),

            (pl.col("ArrDelay") > 15).cast(pl.Int8).alias("is_delayed"),

            (
                pl.col("FlightDate").cast(pl.Utf8)
                + pl.lit("_")
                + pl.col("Flight_Number_Marketing_Airline").cast(pl.Utf8)
                + pl.lit("_")
                + pl.col("Origin").str.strip_chars().str.to_uppercase()
                + pl.lit("_")
                + pl.col("Dest").str.strip_chars().str.to_uppercase()
            ).alias("flight_id"),
        ])
        .filter(pl.col("hour").is_between(0, 23))
        .select([
            "flight_id",
            "year",
            "month",
            "flight_date",
            "origin",
            "dest",
            "route",
            "airline",
            pl.col("Distance").cast(pl.Float64).alias("distance"),
            "hour",
            "day_of_week",
            "season",
            pl.col("DepDelay").cast(pl.Float64).alias("dep_delay"),
            pl.col("ArrDelay").cast(pl.Float64).alias("arr_delay"),
            "is_delayed",
        ])
        .unique(subset=["flight_id"], keep="last")
    )

    print("=== SILVER LAZY QUERY PLAN ===")
    print(silver.explain())

    df = silver.collect()

    print("=== SILVER DATA QUALITY SUMMARY ===")
    print(f"Output silver rows: {df.height}")
    print(f"Output silver columns: {len(df.columns)}")
    print(f"Rows removed: {raw_count - df.height}")
    print(f"Rows kept ratio: {df.height / raw_count:.4f}")

    print("Delay target summary:")
    print(
        df.select([
            pl.col("arr_delay").min().alias("min_arr_delay"),
            pl.col("arr_delay").mean().alias("avg_arr_delay"),
            pl.col("arr_delay").max().alias("max_arr_delay"),
            pl.col("is_delayed").mean().alias("delay_rate"),
        ])
    )

    print("Partition distribution:")
    print(
        df.group_by(["year", "month"])
          .agg(pl.len().alias("rows"))
          .sort(["year", "month"])
    )

    before_version = None

    if not os.path.exists(TARGET):
        write_deltalake(
            TARGET,
            df.to_arrow(),
            mode="overwrite",
            partition_by=["year", "month"],
        )
        print("Created silver table with partition_by=['year', 'month']")
    else:
        dt = DeltaTable(TARGET)
        before_version = dt.version()

        merge_result = (
            dt.merge(
                source=df.to_arrow(),
                predicate="target.flight_id = source.flight_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

        print("Merged into existing silver table")
        print(f"MERGE result: {merge_result}")

    silver_dt = DeltaTable(TARGET)

    print("=== SILVER DELTA TABLE INFO ===")
    if before_version is not None:
        print(f"Silver version before merge: {before_version}")
    print(f"Silver current version: {silver_dt.version()}")
    print(f"Silver schema fields: {len(silver_dt.schema().fields)}")

    print("=== SILVER LAYER FINISHED ===")


if __name__ == "__main__":
    main()
