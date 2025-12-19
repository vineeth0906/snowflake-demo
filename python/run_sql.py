import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    ocsp_fail_open=True,
    insecure_mode=True   # 🔥 critical for CI
)

cursor = conn.cursor()

print("🔄 Loading RAW data using write_pandas()")

customers_df = pd.read_csv("customers.csv")
orders_df = pd.read_csv("orders.csv")

cursor.execute("TRUNCATE TABLE RAW_DB.STAGE.customers_raw")
cursor.execute("TRUNCATE TABLE RAW_DB.STAGE.orders_raw")

write_pandas(
    conn,
    customers_df,
    table_name="customers_raw",
    schema="STAGE",
    database="RAW_DB"
)

write_pandas(
    conn,
    orders_df,
    table_name="orders_raw",
    schema="STAGE",
    database="RAW_DB"
)

print("✅ RAW load completed")

# ---------------------------
# Run SQL layers
# ---------------------------
for file in ["sql/curated.sql", "sql/publish.sql"]:
    print(f"Running {file}")
    with open(file) as f:
        cursor.execute(f.read())

cursor.close()
conn.close()

print("✅ Deployment completed")
