"""Load data to Raw layer"""
import os
import yaml
import snowflake.connector
from pathlib import Path

# Load config
with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Connect and run load steps
print("Connecting to Snowflake...")
sf_user = os.getenv('SNOWFLAKE_USER') or config['snowflake']['user']
sf_password = os.getenv('SNOWFLAKE_PASSWORD') or config['snowflake']['password']
raw_db = config['databases']['raw']

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

    print("\n=== Loading to Raw Layer ===\n")

    # Upload files to stage (validate local files first)
    print("Uploading files to stage...")
    data_dir = Path('data')
    customers_file = data_dir / 'customers.csv'
    orders_file = data_dir / 'orders.csv'
    if not customers_file.exists() or not orders_file.exists():
        raise FileNotFoundError(f"Missing data files in {data_dir}. Ensure customers.csv and orders.csv exist.")

    cursor.execute(f"PUT file://{customers_file.as_posix()} @{raw_db}.STAGE.FILES AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cursor.execute(f"PUT file://{orders_file.as_posix()} @{raw_db}.STAGE.FILES AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    print("✓ Files uploaded")

    # Load customers: select columns explicitly so table default columns (like load_time) are applied
    print("Loading customers...")
    customers_sql = f"""
        COPY INTO {raw_db}.STAGE.customers_raw
    FROM (
        SELECT
            $1::NUMBER      AS customer_id,
            $2::STRING      AS first_name,
            $3::STRING      AS last_name,
            $4::STRING      AS email,
            $5::STRING      AS country,
            $6::DATE        AS signup_date,
            $7::STRING      AS status
        FROM @{raw_db}.STAGE.FILES/customers.csv.gz
    )
    FILE_FORMAT = (
        TYPE='CSV', FIELD_DELIMITER=',', SKIP_HEADER=1, NULL_IF=('NULL','null'),
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
    )
    ON_ERROR='CONTINUE'
    PURGE = TRUE;
    """
    print(customers_sql)
    cursor.execute(customers_sql)
    result = cursor.fetchone()
    loaded_customers = None
    if result and len(result) > 1:
        try:
            loaded_customers = int(result[1])
        except Exception:
            loaded_customers = result[1]
    print(f"✓ Loaded {loaded_customers if loaded_customers is not None else 'unknown'} customers")

    # Load orders
    print("Loading orders...")
    orders_sql = f"""
        COPY INTO {raw_db}.STAGE.orders_raw (
            order_id, customer_id, product, quantity, amount, order_date, status
        )
        FROM @{raw_db}.STAGE.FILES/orders.csv.gz
        FILE_FORMAT = (
            TYPE='CSV', FIELD_DELIMITER=',', SKIP_HEADER=1, NULL_IF=('NULL','null'),
            ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
        )
        ON_ERROR='CONTINUE'
        PURGE = TRUE
    """
    print(orders_sql)
    cursor.execute(orders_sql)
    result = cursor.fetchone()
    loaded_orders = None
    if result and len(result) > 1:
        try:
            loaded_orders = int(result[1])
        except Exception:
            loaded_orders = result[1]
    print(f"✓ Loaded {loaded_orders if loaded_orders is not None else 'unknown'} orders")

    print("\n✅ Raw layer load completed!\n")
except Exception as e:
    # Print a concise error and re-raise for CI visibility
    print(f"Error during load: {e}")
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