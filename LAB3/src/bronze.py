import os
import shutil
import polars as pl

from deltalake import DeltaTable
from deltalake.writer import write_deltalake


SOURCE = "../dataset/flight_data_2018_2024.csv"
TARGET = "../lakehouse/bronze"


def main():
    print("=== BRONZE LAYER START ===")

    if not os.path.exists(SOURCE):
        raise FileNotFoundError(f"Source file not found: {SOURCE}")

    print(f"Reading source CSV: {SOURCE}")

    df = pl.read_csv(SOURCE, infer_schema_length=10000)

    print(f"Raw dataset shape: {df.shape}")
    print(f"Raw columns count: {len(df.columns)}")

    # Remove unnamed empty column from Kaggle CSV export
    if "" in df.columns:
        df = df.drop("")
        print("Dropped empty unnamed column")

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    print(f"Cleaned columns count: {len(df.columns)}")

    if "FlightDate" not in df.columns:
        raise ValueError("FlightDate column is required for daily incremental loading")

    dates = df["FlightDate"].unique().sort().to_list()

    print(f"Incremental batches by FlightDate: {len(dates)}")
    print(f"First batch date: {dates[0]}")
    print(f"Last batch date: {dates[-1]}")

    total_loaded = 0

    for batch_number, flight_date in enumerate(dates, start=1):
        batch = df.filter(pl.col("FlightDate") == flight_date)

        mode = "append" if os.path.exists(TARGET) else "overwrite"

        write_deltalake(
            TARGET,
            batch.to_arrow(),
            mode=mode,
        )

        total_loaded += batch.height

        dt = DeltaTable(TARGET)

        print(
            f"[Bronze batch {batch_number:02d}/{len(dates)}] "
            f"FlightDate={flight_date}, "
            f"rows={batch.height}, "
            f"mode={mode}, "
            f"delta_version={dt.version()}"
        )

    print(f"Total loaded rows: {total_loaded}")

    bronze_dt = DeltaTable(TARGET)

    print("=== BRONZE TABLE INFO BEFORE OPTIMIZE ===")
    print(f"Bronze current version: {bronze_dt.version()}")

    print("=== BRONZE OPTIMIZE COMPACTION ===")
    optimize_result = bronze_dt.optimize.compact()
    print(f"Optimize result: {optimize_result}")

    bronze_dt = DeltaTable(TARGET)

    print("=== BRONZE TABLE INFO AFTER OPTIMIZE ===")
    print(f"Bronze current version: {bronze_dt.version()}")

    print("=== BRONZE LAYER FINISHED ===")


if __name__ == "__main__":
    main()
