import os
import csv
import subprocess
import snowflake.connector

# Generate fresh data
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

print("🔄 Loading RAW data WITHOUT S3")

# Load CUSTOMERS
with open("data/customers.csv", newline="") as f:
    reader = csv.DictReader(f)
    rows = [
        (
            r["CUSTOMER_ID"],
            r["FIRST_NAME"],
            r["LAST_NAME"],
            r["EMAIL"],
            r["COUNTRY"],
            r["SIGNUP_DATE"],
            r["STATUS"]
        )
        for r in reader
    ]

cursor.executemany(
    """
    INSERT INTO RAW_DB.STAGE.CUSTOMERS_RAW
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, COUNTRY, SIGNUP_DATE, STATUS)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """,
    rows
)

# Load ORDERS
with open("data/orders.csv", newline="") as f:
    reader = csv.DictReader(f)
    rows = [
        (
            r["ORDER_ID"],
            r["CUSTOMER_ID"],
            r["ORDER_DATE"],
            r["AMOUNT"],
            r["STATUS"]
        )
        for r in reader
    ]

cursor.executemany(
    """
    INSERT INTO RAW_DB.STAGE.ORDERS_RAW
    (ORDER_ID, CUSTOMER_ID, ORDER_DATE, AMOUNT, STATUS)
    VALUES (%s,%s,%s,%s,%s)
    """,
    rows
)

print("✅ RAW load completed (NO S3 USED)")

print("▶ Running CURATED layer")
run_sql_file("sql/curated.sql")

print("▶ Running PUBLISH layer")
run_sql_file("sql/publish.sql")

cursor.close()
conn.close()

print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
