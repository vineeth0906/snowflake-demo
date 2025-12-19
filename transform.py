import os
import snowflake.connector

print("Connecting to Snowflake...")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
)

cursor = conn.cursor()

print("=== Transforming Data ===")

cursor.execute("TRUNCATE TABLE CURATED_DB.DATA.customers")
cursor.execute("TRUNCATE TABLE CURATED_DB.DATA.orders")

cursor.execute("""
INSERT INTO CURATED_DB.DATA.customers
SELECT DISTINCT * FROM RAW_DB.STAGE.customers_raw
""")

cursor.execute("""
INSERT INTO CURATED_DB.DATA.orders
SELECT DISTINCT * FROM RAW_DB.STAGE.orders_raw
""")

conn.commit()
print("✅ Transform completed")
