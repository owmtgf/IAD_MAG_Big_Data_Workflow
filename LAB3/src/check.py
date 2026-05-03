import polars as pl

df = pl.read_csv("../dataset/flight_data_2018_2024.csv", infer_schema_length=10000)

print(df.columns)
print(df.head())
print(df.shape)
