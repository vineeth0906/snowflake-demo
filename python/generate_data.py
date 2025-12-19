import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

# -----------------------------
# CONFIG
# -----------------------------
CUSTOMER_COUNT = 50
ORDER_COUNT = 120

COUNTRIES = ["US", "UK", "IN", "CA", "AU"]
PRODUCTS = ["Laptop", "Mobile", "Tablet", "Monitor", "Headphones"]
STATUSES_CUSTOMER = ["Active", "Inactive"]
STATUSES_ORDER = ["Shipped", "Pending", "Cancelled"]


# -----------------------------
# CUSTOMER DATA
# -----------------------------
def generate_customers():
    customers = []

    for _ in range(CUSTOMER_COUNT):
        customer_id = random.randint(100000, 999999)

        customers.append({
            "customer_id": customer_id,
            "first_name": f"FN_{customer_id}",
            "last_name": f"LN_{customer_id}",
            "email": f"user_{customer_id}@demo.com",
            "country": random.choice(COUNTRIES),
            "signup_date": (
                datetime.now() - timedelta(days=random.randint(1, 1500))
            ).date(),
            "status": random.choice(STATUSES_CUSTOMER)
        })

    return pd.DataFrame(customers)


# -----------------------------
# ORDER DATA
# -----------------------------
def generate_orders(customers_df):
    orders = []

    customer_ids = customers_df["customer_id"].tolist()

    for _ in range(ORDER_COUNT):
        order_id = uuid.uuid4().int % 1_000_000

        orders.append({
            "order_id": order_id,
            "customer_id": random.choice(customer_ids),
            "product": random.choice(PRODUCTS),
            "quantity": random.randint(1, 5),
            "amount": round(random.uniform(100, 5000), 2),
            "order_date": (
                datetime.now() - timedelta(days=random.randint(1, 365))
            ).date(),
            "status": random.choice(STATUSES_ORDER)
        })

    return pd.DataFrame(orders)


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🔄 Generating fresh demo data...")

    customers_df = generate_customers()
    orders_df = generate_orders(customers_df)

    customers_df.to_csv("customers.csv", index=False)
    orders_df.to_csv("orders.csv", index=False)

    print("✅ New customers.csv and orders.csv generated")
    print(f"   Customers: {len(customers_df)}")
    print(f"   Orders   : {len(orders_df)}")
