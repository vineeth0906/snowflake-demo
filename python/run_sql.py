import os
import pandas as pd
import subprocess
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Generate fresh data every run
os.makedirs("data", exist_ok=True)
subprocess.run(["python", "python/generate_data.py"], check=True)

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    ocsp_fail_open=True
)

cursor = conn.cursor()

def run_sql_file(path):
    with open(path) as f:
        for stmt in f.read().split(";"):
            stmt = stmt.strip()
            if stmt:
                cursor.execute(stmt)

print("🏗️ Creating RAW layer")
run_sql_file("sql/raw.sql")

print("🔄 Loading RAW data")
customers_df = pd.read_csv("data/customers.csv")
orders_df = pd.read_csv("data/orders.csv")

write_pandas(
    conn,
    customers_df,
    "CUSTOMERS_RAW",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

write_pandas(
    conn,
    orders_df,
    "ORDERS_RAW",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

print("✅ RAW load completed")

print("▶ Running CURATED layer")
run_sql_file("sql/curated.sql")

print("▶ Running PUBLISH layer")
run_sql_file("sql/publish.sql")

cursor.close()
conn.close()

print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
