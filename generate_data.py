import csv
import random
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

CUSTOMERS = 100
ORDERS = 300

today = date.today()

# -----------------------
# CUSTOMERS
# -----------------------
with open(DATA_DIR / "customers.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "customer_id",
        "customer_name",
        "email",
        "country",
        "country_code",
        "signup_date",
        "status",
        "loyalty_points"
    ])

    for i in range(1, CUSTOMERS + 1):
        writer.writerow([
            i,
            f"First{i} Last{i}",
            f"customer{i}@email.com",
            random.choice(["US", "UK", "IN"]),
            random.choice(["US", "GB", "IN"]),
            today - timedelta(days=random.randint(1, 1000)),
            random.choice(["Active", "Inactive"]),
            random.randint(0, 500)
        ])

# -----------------------
# ORDERS
# -----------------------
with open(DATA_DIR / "orders.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "order_id",
        "customer_id",
        "product",
        "quantity",
        "amount",
        "order_date",
        "status"
    ])

    for i in range(1, ORDERS + 1):
        qty = random.randint(1, 5)
        price = random.randint(100, 500)
        writer.writerow([
            i,
            random.randint(1, CUSTOMERS),
            random.choice(["Laptop", "Tablet", "Mobile"]),
            qty,
            qty * price,
            today - timedelta(days=random.randint(1, 365)),
            random.choice(["Shipped", "Pending"])
        ])

print("✅ Fresh CSV data generated")
