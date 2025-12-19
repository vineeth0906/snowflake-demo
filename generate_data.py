import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Create data folder
Path("data").mkdir(exist_ok=True)

# Generate Customers
print("Generating customers.csv...")
with open('data/customers.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['customer_id', 'first_name', 'last_name', 'email', 'country', 'signup_date', 'status'])
    
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France']
    for i in range(1, 101):
        signup = datetime.now() - timedelta(days=random.randint(1, 365))
        writer.writerow([
            i,
            f'First{i}',
            f'Last{i}',
            f'customer{i}@email.com',
            random.choice(countries),
            signup.strftime('%Y-%m-%d'),
            random.choice(['Active', 'Inactive'])
        ])

print("✓ Created customers.csv (100 rows)")

# Generate Orders
print("Generating orders.csv...")
with open('data/orders.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['order_id', 'customer_id', 'product', 'quantity', 'amount', 'order_date', 'status'])
    
    products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard']
    for i in range(1, 501):
        order_date = datetime.now() - timedelta(days=random.randint(1, 180))
        qty = random.randint(1, 5)
        amount = round(qty * random.uniform(100, 2000), 2)
        writer.writerow([
            i,
            random.randint(1, 100),
            random.choice(products),
            qty,
            amount,
            order_date.strftime('%Y-%m-%d'),
            random.choice(['Completed', 'Pending', 'Shipped'])
        ])

print("✓ Created orders.csv (500 rows)")
print("\n✅ Data generation completed!")