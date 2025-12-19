import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

def generate_customers(n=50):
    data = []
    for _ in range(n):
        cid = uuid.uuid4().int % 1000000
        data.append({
            "customer_id": cid,
            "customer_name": f"Customer_{cid}",
            "email": f"cust_{cid}@email.com",
            "country": random.choice(["US", "UK", "IN"]),
            "country_code": random.choice(["US", "GB", "IN"]),
            "signup_date": datetime.now() - timedelta(days=random.randint(1, 1000)),
            "status": random.choice(["Active", "Inactive"]),
            "loyalty_points": random.randint(0, 5000)
        })
    return pd.DataFrame(data)

def generate_orders(n=100):
    data = []
    for _ in range(n):
        oid = uuid.uuid4().int % 1000000
        data.append({
            "order_id": oid,
            "customer_id": random.randint(1, 1000000),
            "product": random.choice(["Laptop", "Tablet", "Mobile"]),
            "quantity": random.randint(1, 5),
            "amount": random.randint(500, 5000),
            "order_date": datetime.now() - timedelta(days=random.randint(1, 365)),
            "status": random.choice(["Shipped", "Pending", "Cancelled"])
        })
    return pd.DataFrame(data)

if __name__ == "__main__":
    generate_customers().to_csv("customers.csv", index=False)
    generate_orders().to_csv("orders.csv", index=False)
    print("✅ Fresh data generated")
