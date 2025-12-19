"""
CI SAFE RAW LOAD
- No S3
- No PUT
- No pandas
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

    # -----------------------
    # CUSTOMERS
    # -----------------------
    print("Loading customers...")
    cursor.execute("TRUNCATE TABLE CUSTOMERS_RAW")

    rows = []
    with open(DATA_DIR / "customers.csv") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                int(r["customer_id"]),
                r["customer_name"],
                r["email"],
                r["country"],
                r["country_code"],
                r["signup_date"],
                r["status"],
                int(r["loyalty_points"])
            ))

    cursor.executemany(
        """
        INSERT INTO CUSTOMERS_RAW
        (customer_id, customer_name, email, country,
         country_code, signup_date, status, loyalty_points)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows
    )

    print(f"✓ Loaded {len(rows)} customers")

    # -----------------------
    # ORDERS
    # -----------------------
    print("Loading orders...")
    cursor.execute("TRUNCATE TABLE ORDERS_RAW")

    rows = []
    with open(DATA_DIR / "orders.csv") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                int(r["order_id"]),
                int(r["customer_id"]),
                r["product"],
                int(r["quantity"]),
                int(r["amount"]),
                r["order_date"],
                r["status"]
            ))

    cursor.executemany(
        """
        INSERT INTO ORDERS_RAW
        (order_id, customer_id, product,
         quantity, amount, order_date, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows
    )

    print(f"✓ Loaded {len(rows)} orders")

    conn.commit()
    print("✅ RAW layer load completed successfully")

except Exception as e:
    print("❌ RAW load failed:", e)
    raise

finally:
    cursor.close()
    conn.close()
