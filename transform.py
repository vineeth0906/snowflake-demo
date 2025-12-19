"""Transform data from RAW to CURATED and PUBLISH layers"""

import os
import yaml
import snowflake.connector

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
    ocsp_fail_open=True
)

cursor = conn.cursor()

try:
    print("\n=== Transforming Data ===")

    # ----------------------------
    # CURATED: CUSTOMERS
    # ----------------------------
    print("Transforming customers to curated layer...")

    cursor.execute("USE DATABASE CURATED_DB")
    cursor.execute("USE SCHEMA DATA")

    cursor.execute("TRUNCATE TABLE CUSTOMERS")

    cursor.execute("""
        INSERT INTO CUSTOMERS
        SELECT DISTINCT
            customer_id,
            first_name || ' ' || last_name AS full_name,
            email,
            country,
            CASE
                WHEN country = 'USA' THEN 'US'
                WHEN country = 'UK' THEN 'GB'
                ELSE 'OT'
            END AS country_code,
            DATEDIFF(day, signup_date, CURRENT_DATE()) AS days_since_signup,
            status,
            0 AS is_deleted
        FROM RAW_DB.STAGE.CUSTOMERS_RAW
    """)

    print("✓ Customers curated")

    # ----------------------------
    # CURATED: ORDERS
    # ----------------------------
    print("Transforming orders to curated layer...")

    cursor.execute("TRUNCATE TABLE ORDERS")

    cursor.execute("""
        INSERT INTO ORDERS
        SELECT DISTINCT
            order_id,
            customer_id,
            product,
            quantity,
            amount,
            order_date,
            status
        FROM RAW_DB.STAGE.ORDERS_RAW
    """)

    print("✓ Orders curated")

    # ----------------------------
    # PUBLISH: ANALYTICS
    # ----------------------------
    print("Building publish layer...")

    cursor.execute("USE DATABASE PUBLISH_DB")
    cursor.execute("USE SCHEMA ANALYTICS")

    cursor.execute("TRUNCATE TABLE CUSTOMER_SUMMARY")

    cursor.execute("""
        INSERT INTO CUSTOMER_SUMMARY
        SELECT
            c.customer_id,
            c.full_name,
            c.country,
            COUNT(o.order_id) AS total_orders,
            COALESCE(SUM(o.amount), 0) AS total_spent
        FROM CURATED_DB.DATA.CUSTOMERS c
        LEFT JOIN CURATED_DB.DATA.ORDERS o
            ON c.customer_id = o.customer_id
        GROUP BY
            c.customer_id,
            c.full_name,
            c.country
    """)

    print("✓ Customer summary published")

    conn.commit()
    print("\n✅ Transform completed successfully")

except Exception as e:
    print("Error during transform:", e)
    raise

finally:
    cursor.close()
    conn.close()
