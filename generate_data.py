import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path

# Output directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Unique seed per run (IMPORTANT)
run_id = int(datetime.utcnow().timestamp())

num_customers = 100
num_orders = 300

countries = [
    ("USA", "US"), ("India", "IN"), ("UK", "GB"),
    ("Canada", "CA"), ("Germany", "DE")
]

products = ["Laptop", "Mobile", "Tablet", "Headphones"]

# -----------------------
# Customers
# -----------------------
customers = []
for i in range(num_customers):
    customer_id = run_id + i
    country, country_code = random.choice(countries)

    customers.append({
        "customer_id": customer_id,
        "customer_name": f"First{i} Last{i}",
        "email": f"customer{customer_id}@email.com",
        "country": country,
        "country_code": country_code,
        "signup_date": datetime.utcnow().date(),
        "status": random.choice(["Active", "Inactive"]),
        "loyalty_points": random.randint(0, 5000)
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv(DATA_DIR / "customers.csv", index=False)

# -----------------------
# Orders (INTENTIONALLY MULTIPLE PER CUSTOMER)
# -----------------------
orders = []
for i in range(num_orders):
    order_id = run_id + i  # unique per run
    customer = random.choice(customers)

    orders.append({
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "amount": random.randint(100, 2000),
        "order_date": datetime.utcnow() - timedelta(days=random.randint(0, 30)),
        "status": random.choice(["Shipped", "Pending", "Cancelled"])
    })

orders_df = pd.DataFrame(orders)
orders_df.to_csv(DATA_DIR / "orders.csv", index=False)

print("✅ New customer & order data generated")
