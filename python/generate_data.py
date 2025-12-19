import pandas as pd
import random
from datetime import date
import os

os.makedirs("data", exist_ok=True)

customers = []
for i in range(1, 11):
    customers.append({
        "CUSTOMER_ID": i,
        "FIRST_NAME": f"First{i}",
        "LAST_NAME": f"Last{i}",
        "EMAIL": f"user{i}@mail.com",
        "COUNTRY": random.choice(["US", "IN", "UK"]),
        "SIGNUP_DATE": date.today(),
        "STATUS": "ACTIVE"
    })

orders = []
for i in range(1, 21):
    orders.append({
        "ORDER_ID": i,
        "CUSTOMER_ID": random.randint(1, 10),
        "ORDER_DATE": date.today(),
        "AMOUNT": random.randint(100, 5000),
        "STATUS": "COMPLETED"
    })

pd.DataFrame(customers).to_csv("data/customers.csv", index=False)
pd.DataFrame(orders).to_csv("data/orders.csv", index=False)

print("✅ Data generated successfully")
