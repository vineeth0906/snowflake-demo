import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.util_text import execute_string

# --------------------------------------------------
# Snowflake connection
# --------------------------------------------------
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    ocsp_fail_open=True,
    insecure_mode=True
)

cursor = conn.cursor()

# --------------------------------------------------
# 1️⃣ CREATE RAW LAYER (DDL)
# --------------------------------------------------
print("🏗️ Creating RAW layer objects")

with open("sql/raw.sql", "r") as f:
    execute_string(conn, f.read())

# --------------------------------------------------
# 2️⃣ LOAD RAW DATA (NO S3, NO PUT)
# --------------------------------------------------
print("🔄 Loading RAW data using write_pandas()")

customers_df = pd.read_csv("customers.csv")
orders_df = pd.read_csv("orders.csv")

# Ensure clean reloads
cursor.execute("TRUNCATE TABLE IF EXISTS RAW_DB.STAGE.customers_raw")
cursor.execute("TRUNCATE TABLE IF EXISTS RAW_DB.STAGE.orders_raw")

write_pandas(
    conn,
    customers_df,
    table_name="customers_raw",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

write_pandas(
    conn,
    orders_df,
    table_name="orders_raw",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

print("✅ RAW load completed")

# --------------------------------------------------
# 3️⃣ CURATED + PUBLISH LAYERS
# --------------------------------------------------
for sql_file in ["sql/curated.sql", "sql/publish.sql"]:
    print(f"▶ Executing {sql_file}")
    with open(sql_file, "r") as f:
        execute_string(conn, f.read())

# --------------------------------------------------
# Cleanup
# --------------------------------------------------
cursor.close()
conn.close()

print("✅ Snowflake deployment completed successfully")
