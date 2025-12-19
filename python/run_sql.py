print("🔄 Loading RAW data")

customers_df = pd.read_csv("data/customers.csv")
orders_df = pd.read_csv("data/orders.csv")

write_pandas(
    conn,
    customers_df,
    table_name="CUSTOMERS_RAW",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

write_pandas(
    conn,
    orders_df,
    table_name="ORDERS_RAW",
    database="RAW_DB",
    schema="STAGE",
    auto_create_table=False
)

print("✅ RAW load completed")
