import polars as pl
import sys

# Set console encoding to UTF-8
sys.stdout.reconfigure(encoding="utf-8")

# Read the parquet file
df = pl.read_parquet("D:/Pythonstuff/test/divpower2/polars2/transactions.parquet")

# Print basic DataFrame info
print("\nDataFrame shape:", df.shape)
print("\nColumn names:", df.columns)
print("\nColumn data types:")
for col in df.columns:
    print(f"{col}: {df[col].dtype}")

# ?Optional Query 1: Show all transactions sorted by CUST_ID (raw data)
"""
result = df.select([pl.col("CUST_ID"), pl.col("YEAR"), pl.col("AMOUNT").floor()]).sort(
    "CUST_ID"
)
print("\nTotal number of records:", len(result))
print("\nFirst 100 records:")
print(result.head(100))
"""

# ?Optional Query 2: Show total spending per customer and year
"""
result = df.group_by(["CUST_ID", "YEAR"]).agg([
    pl.col("AMOUNT").sum().floor().alias("TOTAL_AMOUNT")
]).sort(["CUST_ID", "YEAR"])
print("\nTotal number of records:", len(result))
print("\nFirst 100 records:")
print(result.head(100))
"""

# ?Optional Query 3: Show total spending per customer (no year breakdown)
"""
result = df.group_by("CUST_ID").agg([
    pl.col("AMOUNT").sum().floor().alias("TOTAL_AMOUNT")
]).sort("CUST_ID")
print("\nTotal number of records:", len(result))
print("\nFirst 100 records:")
print(result.head(100))
"""

# Query 4: Show top 10 spenders (current active query)
result = (
    df.group_by("CUST_ID")
    .agg([pl.col("AMOUNT").sum().floor().alias("TOTAL_AMOUNT")])
    .sort("TOTAL_AMOUNT", descending=True)
)

print("\nTotal number of records:", len(result))
print("\nTop 10 Spenders:")
print(result.head(10))
