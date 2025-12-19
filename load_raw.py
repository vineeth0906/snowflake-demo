"""
Load data into RAW layer
CI SAFE: No PUT, No S3, No write_pandas
"""

import os
import csv
import snowflake.connector
from pathlib import Path

DATA_DIR = Path("data")

print("Connecting to Snowflake...")

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database="RAW_DB",
    schema="STAGE"
)

cursor = conn.cursor()

try:
    print("=== Loading to Raw Layer (NO S3, CI SAFE) ===")

    # -------------------------
    # LOAD CUSTOMERS
    # -------------------------
    print("Loading customers...")
    cursor.execute("TRUNCATE TABLE CUSTOMERS_RAW")

    customers_file = DATA_DIR / "customers.csv"
    with open(customers_file, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        rows = [
            (
                int(r[0]),   # customer_id
                r[1],        # customer_name
                r[2],        # email
                r[3],        # country
                r[4],        # country_code
                r[5],        # signup_date
                r[6],        # status
                int(r[7])    # loyalty_points
            )
            for r in reader
        ]

    cursor.executemany(
        """
        INSERT INTO CUSTOMERS_RAW
        (customer_id, customer_name, email, country, country_code,
         signup_date, status, loyalty_points)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows
    )

    print(f"✓ Loaded {len(rows)} customers")

    # -------------------------
    # LOAD ORDERS
    # -------------------------
    print("Loading orders...")
    cursor.execute("TRUNCATE TABLE ORDERS_RAW")

    orders_file = DATA_DIR / "orders.csv"
    with open(orders_file, newline="") as f:
        reader = csv.reader(f)
        next(reader)

        rows = [
            (
                int(r[0]),   # order_id
                int(r[1]),   # customer_id
                r[2],        # product
                int(r[3]),   # quantity
                int(r[4]),   # amount
                r[5],        # order_date
                r[6]         # status
            )
            for r in reader
        ]

    cursor.executemany(
        """
        INSERT INTO ORDERS_RAW
        (order_id, customer_id, product, quantity,
         amount, order_date, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows
    )

    print(f"✓ Loaded {len(rows)} orders")

    conn.commit()
    print("✅ RAW layer load completed successfully!")

except Exception as e:
    print("❌ Error during load:", e)
    raise

finally:
    cursor.close()
    conn.close()
