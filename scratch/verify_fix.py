import sys

import polars as pl

parquet_path = "data/parquet/주택인허가_동별개요.parquet"
pk_value = "1017100005635"

df = pl.read_parquet(parquet_path)
result = df.filter(pl.col("관리_동별_개요_PK") == pk_value)

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

print(f"Checking PK: {pk_value}")
if result.height == 0:
    print("Record not found!")
else:
    # Print as a dictionary to avoid table drawing issues
    print(result.to_dicts()[0])
