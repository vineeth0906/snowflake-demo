import snowflake.connector
import os

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    ocsp_fail_open=True
)

cursor = conn.cursor()

for file in ["sql/raw.sql", "sql/curated.sql", "sql/publish.sql"]:
    print(f"Running {file}")
    with open(file) as f:
        cursor.execute(f.read())

cursor.close()
conn.close()

print("✅ Deployment completed")
