"""Transform data through layers"""
import os
import yaml
import snowflake.connector
from dotenv import load_dotenv

# Load config and optional .env
load_dotenv()
with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Prefer non-empty env vars
sf_user = os.getenv('SNOWFLAKE_USER') or config['snowflake']['user']
sf_password = os.getenv('SNOWFLAKE_PASSWORD') or config['snowflake']['password']

print("Connecting to Snowflake...")
conn = None
cursor = None
try:
    conn = snowflake.connector.connect(
        account=config['snowflake']['account'],
        user=sf_user,
        password=sf_password,
        role=config['snowflake']['role'],
        warehouse=config['snowflake']['warehouse']
    )
    cursor = conn.cursor()

    raw_db = config['databases']['raw']
    curated_db = config['databases']['curated']
    publish_db = config['databases']['publish']

    print("\n=== Transforming Data ===\n")

    # Transform Customers to Curated
    print("Transforming customers to curated layer...")
    cursor.execute(f"""
        MERGE INTO {curated_db}.DATA.customers tgt
        USING (
            SELECT 
                customer_id,
                CONCAT(first_name, ' ', last_name) AS full_name,
                LOWER(email) AS email,
                country,
                CASE country 
                    WHEN 'USA' THEN 'US'
                    WHEN 'UK' THEN 'GB'
                    WHEN 'Canada' THEN 'CA'
                    WHEN 'Germany' THEN 'DE'
                    WHEN 'France' THEN 'FR'
                    ELSE NULL
                END AS country_code,
                signup_date,
                status,
                CASE WHEN status = 'Active' THEN TRUE ELSE FALSE END AS is_active
            FROM {raw_db}.STAGE.customers_raw
        ) src
        ON tgt.customer_id = src.customer_id
        WHEN MATCHED THEN UPDATE SET
            tgt.full_name = src.full_name,
            tgt.status = src.status,
            tgt.is_active = src.is_active
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.customer_id, src.full_name, src.email, src.country, 
            src.country_code, src.signup_date, src.status, src.is_active
        )
    """)
    conn.commit()
    print(f"✓ Customers transformed")

    # Transform Orders to Curated
    print("Transforming orders to curated layer...")
    cursor.execute(f"""
        MERGE INTO {curated_db}.DATA.orders tgt
        USING (
            SELECT 
                order_id,
                customer_id,
                product,
                quantity,
                amount,
                ROUND(amount * 0.10, 2) AS tax,
                ROUND(amount * 1.10, 2) AS total,
                order_date,
                status
            FROM {raw_db}.STAGE.orders_raw
        ) src
        ON tgt.order_id = src.order_id
        WHEN MATCHED THEN UPDATE SET
            tgt.status = src.status
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.order_id, src.customer_id, src.product, src.quantity, 
            src.amount, src.tax, src.total, src.order_date, src.status
        )
    """)
    conn.commit()
    print(f"✓ Orders transformed")

    # Create Customer Summary (Publish Layer)
    print("Creating customer summary...")
    cursor.execute(f"""
        MERGE INTO {publish_db}.ANALYTICS.customer_summary tgt
        USING (
            SELECT 
                c.customer_id,
                c.full_name,
                c.country,
                COUNT(o.order_id) AS total_orders,
                COALESCE(SUM(o.total), 0) AS total_spent,
                COALESCE(AVG(o.total), 0) AS avg_order_value,
                CASE 
                    WHEN COUNT(o.order_id) >= 10 THEN 'VIP'
                    WHEN COUNT(o.order_id) >= 5 THEN 'Regular'
                    ELSE 'New'
                END AS customer_segment
            FROM {curated_db}.DATA.customers c
            LEFT JOIN {curated_db}.DATA.orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.full_name, c.country
        ) src
        ON tgt.customer_id = src.customer_id
        WHEN MATCHED THEN UPDATE SET
            tgt.total_orders = src.total_orders,
            tgt.total_spent = src.total_spent,
            tgt.avg_order_value = src.avg_order_value,
            tgt.customer_segment = src.customer_segment
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.customer_id, src.full_name, src.country, src.total_orders,
            src.total_spent, src.avg_order_value, src.customer_segment
        )
    """)
    conn.commit()
    print(f"✓ Customer summary created")

    # Create Daily Sales (Publish Layer)
    print("Creating daily sales summary...")
    cursor.execute(f"""
        MERGE INTO {publish_db}.ANALYTICS.daily_sales tgt
        USING (
            SELECT 
                order_date,
                COUNT(order_id) AS total_orders,
                SUM(total) AS total_revenue,
                AVG(total) AS avg_order_value
            FROM {curated_db}.DATA.orders
            GROUP BY order_date
        ) src
        ON tgt.order_date = src.order_date
        WHEN MATCHED THEN UPDATE SET
            tgt.total_orders = src.total_orders,
            tgt.total_revenue = src.total_revenue,
            tgt.avg_order_value = src.avg_order_value
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.order_date, src.total_orders, src.total_revenue, src.avg_order_value
        )
    """)
    conn.commit()
    print(f"✓ Daily sales summary created")

    print("\n✅ All transformations completed!\n")
except Exception as e:
    print(f"Error during transform: {e}")
    raise
finally:
    try:
        if cursor:
            cursor.close()
    except Exception:
        pass
    try:
        if conn:
            conn.close()
    except Exception:
        pass