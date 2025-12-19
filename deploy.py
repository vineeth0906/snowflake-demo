"""Deploy Snowflake infrastructure"""
import os
import yaml
import snowflake.connector
try:
    from dotenv import load_dotenv
    # Load .env (optional) so CI/local .env values can override config
    load_dotenv()
except Exception:
    # python-dotenv not installed; continue without loading .env
    pass

# Load config
with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Connect
print("Connecting to Snowflake...")
# Prefer non-empty environment variables, otherwise fall back to config
sf_user = os.getenv('SNOWFLAKE_USER') or config['snowflake']['user']
sf_password = os.getenv('SNOWFLAKE_PASSWORD') or config['snowflake']['password']

conn = snowflake.connector.connect(
    account=config['snowflake']['account'],
    user=sf_user,
    password=sf_password,
    role=config['snowflake']['role'],
    warehouse=config['snowflake']['warehouse']
)
cursor = conn.cursor()

print("\n=== Creating Infrastructure ===\n")

# Create Databases
print("Creating Databases...")
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['databases']['raw']}")
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['databases']['curated']}")
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['databases']['publish']}")
print("✓ Databases created")

# Create Schemas
print("Creating Schemas...")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {config['databases']['raw']}.STAGE")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {config['databases']['curated']}.DATA")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {config['databases']['publish']}.ANALYTICS")
print("✓ Schemas created")

# Create Stage & File Format
print("Creating Stage...")
cursor.execute(f"""
    CREATE STAGE IF NOT EXISTS {config['databases']['raw']}.STAGE.FILES
""")
cursor.execute(f"""
    CREATE FILE FORMAT IF NOT EXISTS {config['databases']['raw']}.STAGE.CSV_FORMAT
    TYPE='CSV' FIELD_DELIMITER=',' SKIP_HEADER=1 NULL_IF=('NULL','null','')
    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
""")
print("✓ Stage created")

# RAW Tables
print("Creating Raw Tables...")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['databases']['raw']}.STAGE.customers_raw (
        customer_id INT,
        first_name VARCHAR,
        last_name VARCHAR,
        email VARCHAR,
        country VARCHAR,
        signup_date DATE,
        status VARCHAR,
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
""")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['databases']['raw']}.STAGE.orders_raw (
        order_id INT,
        customer_id INT,
        product VARCHAR,
        quantity INT,
        amount DECIMAL(10,2),
        order_date DATE,
        status VARCHAR,
        load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
""")
print("✓ Raw tables created")

# CURATED Tables
print("Creating Curated Tables...")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['databases']['curated']}.DATA.customers (
        customer_id INT PRIMARY KEY,
        full_name VARCHAR,
        email VARCHAR,
        country VARCHAR,
        country_code VARCHAR,
        signup_date DATE,
        status VARCHAR,
        is_active BOOLEAN
    )
""")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['databases']['curated']}.DATA.orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        product VARCHAR,
        quantity INT,
        amount DECIMAL(10,2),
        tax DECIMAL(10,2),
        total DECIMAL(10,2),
        order_date DATE,
        status VARCHAR
    )
""")
print("✓ Curated tables created")

# PUBLISH Tables
print("Creating Publish Tables...")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['databases']['publish']}.ANALYTICS.customer_summary (
        customer_id INT PRIMARY KEY,
        full_name VARCHAR,
        country VARCHAR,
        total_orders INT,
        total_spent DECIMAL(15,2),
        avg_order_value DECIMAL(10,2),
        customer_segment VARCHAR
    )
""")
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['databases']['publish']}.ANALYTICS.daily_sales (
        order_date DATE PRIMARY KEY,
        total_orders INT,
        total_revenue DECIMAL(15,2),
        avg_order_value DECIMAL(10,2)
    )
""")
print("✓ Publish tables created")

print("\n✅ Infrastructure deployment completed!\n")
conn.close()