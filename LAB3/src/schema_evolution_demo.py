from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import polars as pl

PATH = "../lakehouse/silver"

print("=== SCHEMA EVOLUTION START ===")

dt_before = DeltaTable(PATH)

print("Before evolution:")
print(f"Version: {dt_before.version()}")
print(f"Columns: {[f.name for f in dt_before.schema().fields]}")

df = pl.read_delta(PATH)

df2 = df.with_columns([
    pl.lit(1).alias("new_feature")
])

write_deltalake(
    PATH,
    df2.to_arrow(),
    mode="overwrite",
    schema_mode="merge"
)

dt_after = DeltaTable(PATH)

print("\nAfter evolution:")
print(f"Version: {dt_after.version()}")
print(f"Columns: {[f.name for f in dt_after.schema().fields]}")

print("\nPreview new column:")
print(pl.read_delta(PATH).select(["new_feature"]).head())

print("=== SCHEMA EVOLUTION FINISHED ===")
