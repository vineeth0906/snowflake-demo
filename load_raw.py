"""Load data to Raw layer"""
import os
import yaml
import snowflake.connector
from pathlib import Path

# Load config
with open('config.yaml') as f:
    config = yaml.safe_load(f)

print("Connecting to Snowflake...")

sf_user = os.getenv('SNOWFLAKE_USER') or config['snowflake']['user']
sf_password = os.getenv('SNOWFLAKE_PASSWORD') or config['snowflake']['password']
raw_db = config['databases']['raw']

conn = None
cursor = None

try:
    # 🔑 CRITICAL FIX: explicitly disable OCSP + S3 accelerate
    conn = snowflake.connector.connect(
        account=config['snowflake']['account'],
        user=sf_user,
        password=sf_password,
        role=config['snowflake']['role'],
        warehouse=config['snowflake']['warehouse'],
        ocsp_fail_open=True,
        client_session_keep_alive=True
    )

    cursor = conn.cursor()

    print("\n=== Loading to Raw Layer ===\n")

    # Validate files
    data_dir = Path("data")
    customers_file = data_dir / "customers.csv"
    orders_file = data_dir / "orders.csv"

    if not customers_file.exists() or not orders_file.exists():
        raise FileNotFoundError("customers.csv or orders.csv not found")

    # Upload files
    print("Uploading files to stage...")
    cursor.execute(
        f"PUT file://{customers_file.as_posix()} @{raw_db}.STAGE.FILES AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    cursor.execute(
        f"PUT file://{orders_file.as_posix()} @{raw_db}.STAGE.FILES AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    print("✓ Files uploaded")

    # ------------------------
    # Load CUSTOMERS (positional mapping)
    # ------------------------
    print("Loading customers...")
    customers_sql = f"""
        COPY INTO {raw_db}.STAGE.customers_raw
        FROM (
            SELECT
                $1::NUMBER,
                $2::STRING,
                $3::STRING,
                $4::STRING,
                $5::STRING,
                $6::DATE,
                $7::STRING
            FROM @{raw_db}.STAGE.FILES/customers.csv.gz
        )
        FILE_FORMAT = (
            TYPE='CSV'
            FIELD_DELIMITER=','
            SKIP_HEADER=1
            NULL_IF=('NULL','null','')
        )
        ON_ERROR='CONTINUE'
        PURGE=TRUE;
    """
    cursor.execute(customers_sql)
    res = cursor.fetchone()
    print(f"✓ Loaded customers: {res[1] if res else 'unknown'}")

    # ------------------------
    # Load ORDERS (MATCHING FIX)
    # ------------------------
    print("Loading orders...")
    orders_sql = f"""
        COPY INTO {raw_db}.STAGE.orders_raw
        FROM (
            SELECT
                $1::NUMBER,
                $2::NUMBER,
                $3::STRING,
                $4::NUMBER,
                $5::NUMBER(10,2),
                $6::DATE,
                $7::STRING
            FROM @{raw_db}.STAGE.FILES/orders.csv.gz
        )
        FILE_FORMAT = (
            TYPE='CSV'
            FIELD_DELIMITER=','
            SKIP_HEADER=1
            NULL_IF=('NULL','null','')
        )
        ON_ERROR='CONTINUE'
        PURGE=TRUE;
    """
    cursor.execute(orders_sql)
    res = cursor.fetchone()
    print(f"✓ Loaded orders: {res[1] if res else 'unknown'}")

    print("\n✅ Raw layer load completed!\n")

except Exception as e:
    print(f"❌ Error during load: {e}")
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
