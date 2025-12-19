import os
import io
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.util_text import split_statements

# --------------------------------------------------
# Connect to Snowflake
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
# Helper: run multi-statement SQL files (3.6.0 SAFE)
# --------------------------------------------------
def run_sql_file(path):
    with open(path, "r") as f:
        sql_text = f.read()

    buffer = io.StringIO(sql_text)

    for stmt, _ in split_statements(buffer):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)

# --------------------------------------------------
# 1️⃣ RAW LAYER (DDL)
# --------------------------------------------------
print("🏗️ Creating RAW layer objects")
run_sql_file("sql/raw.sql")

# --------------------------------------------------
# 2️⃣ LOAD RAW DATA (NO S3 / NO PUT)
# --------------------------------------------------
print("🔄 Loading RAW data using write_pandas()")

customers_df = pd.read_csv("customers.csv")
orders_df = pd.read_csv("orders.csv")

cursor.execute("TRUNCATE TABLE IF EXISTS RAW_DB.STAGE.CUSTOMERS_RAW")
cursor.execute("TRUNCATE TABLE IF EXISTS RAW_DB.STAGE.ORDERS_RAW")

write_pandas(
    conn,
    customers_df,
    table_name="CUSTOMERS_RAW",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

write_pandas(
    conn,
    orders_df,
    table_name="ORDERS_RAW",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

print("✅ RAW load completed")

# --------------------------------------------------
# 3️⃣ CURATED + PUBLISH
# --------------------------------------------------
print("▶ Running CURATED layer")
run_sql_file("sql/curated.sql")

print("▶ Running PUBLISH layer")
run_sql_file("sql/publish.sql")

# --------------------------------------------------
# Cleanup
# --------------------------------------------------
cursor.close()
conn.close()

print("✅ Snowflake deployment completed successfully")
