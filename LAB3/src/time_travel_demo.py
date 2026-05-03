import polars as pl
from deltalake import DeltaTable

PATH = "../lakehouse/bronze"

dt = DeltaTable(PATH)

print("Current version:", dt.version())

old = pl.read_delta(PATH, version=0)

print("Old version (0) shape:", old.shape)
print(old.head())
