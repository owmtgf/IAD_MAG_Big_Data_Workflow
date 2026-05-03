import polars as pl
from deltalake import DeltaTable

SILVER_PATH = "../lakehouse/silver"
GOLD_FEATURES_PATH = "../lakehouse/gold/features"

print("=== Delta table history / versions ===")
silver_dt = DeltaTable(SILVER_PATH)
print("Silver current version:", silver_dt.version())

gold_dt = DeltaTable(GOLD_FEATURES_PATH)
print("Gold features current version:", gold_dt.version())

print("\n=== Time travel example ===")
old_version = 0

old_silver = pl.read_delta(
    SILVER_PATH,
    version=old_version
)

print(f"Read silver table version {old_version}")
print(old_silver.shape)

print("\n=== OPTIMIZE / compaction ===")
silver_dt.optimize.compact()
print("Silver table compacted")

print("\n=== VACUUM ===")
silver_dt.vacuum(
    retention_hours=168,
    dry_run=True
)
print("Vacuum dry run completed")

print("\nDelta operations finished")