"""Load data to Raw layer using write_pandas (CI-safe)"""

import os
import yaml
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

print("Connecting to Snowflake...")

conn = snowflake.connector.connect(
    account=config["snowflake"]["account"],
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=config["snowflake"]["role"],
    warehouse=config["snowflake"]["warehouse"],
    database=config["databases"]["raw"],
    schema="STAGE",
    ocsp_fail_open=True
)

print("\n=== Loading to Raw Layer (CI SAFE) ===\n")

# Paths
data_dir = Path("data")
customers_file = data_dir / "customers.csv"
orders_file = data_dir / "orders.csv"

# Load CSVs
customers_df = pd.read_csv(customers_file)
orders_df = pd.read_csv(orders_file)

# Write customers
print("Loading customers...")
success, nchunks, nrows, _ = write_pandas(
    conn,
    customers_df,
    table_name="CUSTOMERS_RAW",
    overwrite=True
)
print(f"✓ Customers loaded: {nrows}")

# Write orders
print("Loading orders...")
success, nchunks, nrows, _ = write_pandas(
    conn,
    orders_df,
    table_name="ORDERS_RAW",
    overwrite=True
)
print(f"✓ Orders loaded: {nrows}")

print("\n✅ Raw layer load completed successfully\n")

conn.close()
