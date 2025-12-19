import csv
import os
import snowflake.connector
import yaml

print("Connecting to Snowflake...")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
)

cursor = conn.cursor()

print("=== Loading to Raw Layer (CI SAFE) ===")

cursor.execute("TRUNCATE TABLE RAW_DB.STAGE.customers_raw")
cursor.execute("TRUNCATE TABLE RAW_DB.STAGE.orders_raw")

with open("customers.csv") as f:
    rows = list(csv.reader(f))

cursor.executemany(
    "INSERT INTO RAW_DB.STAGE.customers_raw VALUES (%s,%s,%s,%s)",
    rows
)

with open("orders.csv") as f:
    rows = list(csv.reader(f))

cursor.executemany(
    "INSERT INTO RAW_DB.STAGE.orders_raw VALUES (%s,%s,%s,%s)",
    rows
)

conn.commit()
print("✅ Raw load completed")
