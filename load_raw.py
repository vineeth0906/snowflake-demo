"""Load data to Raw layer"""
import os
import yaml
import snowflake.connector
from pathlib import Path

# Load config
with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Connect
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=config['snowflake']['account'],
    user=os.getenv('SNOWFLAKE_USER', config['snowflake']['user']),
    password=os.getenv('SNOWFLAKE_PASSWORD', config['snowflake']['password']),
    role=config['snowflake']['role'],
    warehouse=config['snowflake']['warehouse']
)
cursor = conn.cursor()

print("\n=== Loading to Raw Layer ===\n")

raw_db = config['databases']['raw']

# Upload files to stage
print("Uploading files to stage...")
cursor.execute(f"PUT file://data/customers.csv @{raw_db}.STAGE.FILES AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
cursor.execute(f"PUT file://data/orders.csv @{raw_db}.STAGE.FILES AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
print("✓ Files uploaded")

# Load customers
print("Loading customers...")
cursor.execute(f"""
    COPY INTO {raw_db}.STAGE.customers_raw
    FROM @{raw_db}.STAGE.FILES/customers.csv.gz
    FILE_FORMAT = (FORMAT_NAME = '{raw_db}.STAGE.CSV_FORMAT')
    PURGE = TRUE
""")
result = cursor.fetchone()
print(f"✓ Loaded {result[1]} customers")

# Load orders
print("Loading orders...")
cursor.execute(f"""
    COPY INTO {raw_db}.STAGE.orders_raw
    FROM @{raw_db}.STAGE.FILES/orders.csv.gz
    FILE_FORMAT = (FORMAT_NAME = '{raw_db}.STAGE.CSV_FORMAT')
    PURGE = TRUE
""")
result = cursor.fetchone()
print(f"✓ Loaded {result[1]} orders")

print("\n✅ Raw layer load completed!\n")
conn.close()