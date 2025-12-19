"""Load data to Raw layer using pure SQL INSERT (CI SAFE)"""

import os
import yaml
import pandas as pd
import snowflake.connector
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

cursor = conn.cursor()

print("\n=== Loading to Raw Layer (NO S3, CI SAFE) ===\n")

# Read CSVs
data_dir = Path("data")
customers_df = pd.read_csv(data_dir / "customers.csv")
orders_df = pd.read_csv(data_dir / "orders.csv")

# -----------------------
# Load CUSTOMERS
# -----------------------
print("Loading customers...")

customers_sql = """
INSERT INTO CUSTOMERS_RAW
(customer_id, first_name, last_name, email, country, signup_date, status)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(
    customers_sql,
    customers_df.values.tolist()
)

print(f"✓ Customers inserted: {len(customers_df)}")

# -----------------------
# Load ORDERS
# -----------------------
print("Loading orders...")

orders_sql = """
INSERT INTO ORDERS_RAW
(order_id, customer_id, product, quantity, amount, order_date, status)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(
    orders_sql,
    orders_df.values.tolist()
)

print(f"✓ Orders inserted: {len(orders_df)}")

conn.commit()
cursor.close()
conn.close()

print("\n✅ Raw layer load completed successfully (NO S3 USED)\n")
