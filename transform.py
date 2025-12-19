import os
import snowflake.connector

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database="CURATED_DB",
    schema="DATA"
)

cursor = conn.cursor()

try:
    print("=== Transforming Data ===")

    # ----------------------------
    # CUSTOMERS
    # ----------------------------
    print("Transforming customers to curated layer...")
    cursor.execute("TRUNCATE TABLE CUSTOMERS")

    cursor.execute("""
        INSERT INTO CUSTOMERS
        SELECT DISTINCT
            customer_id,
            customer_name,
            email,
            country,
            country_code,
            signup_date,
            status,
            loyalty_points
        FROM RAW_DB.STAGE.CUSTOMERS_RAW
    """)

    print("✓ Customers transformed")

    # ----------------------------
    # ORDERS (DEDUP BY BUSINESS KEY)
    # ----------------------------
    print("Transforming orders to curated layer...")
    cursor.execute("TRUNCATE TABLE ORDERS")

    cursor.execute("""
        INSERT INTO ORDERS
        SELECT
            order_id,
            customer_id,
            product,
            quantity,
            amount,
            order_date,
            status
        FROM (
            SELECT
                order_id,
                customer_id,
                product,
                quantity,
                amount,
                order_date,
                status,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY order_date DESC
                ) AS rn
            FROM RAW_DB.STAGE.ORDERS_RAW
        )
        WHERE rn = 1
    """)

    print("✓ Orders transformed")

    conn.commit()
    print("✅ Transformation completed successfully!")

except Exception as e:
    print("Error during transform:", e)
    raise

finally:
    cursor.close()
    conn.close()
