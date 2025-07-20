import requests
import random
from datetime import datetime, timedelta
import csv
import json

# Fetch products data
products_response = requests.get('https://dummyjson.com/products?limit=100')
products_data = products_response.json()
products = products_data['products']

# Fetch users data
users_response = requests.get('https://dummyjson.com/users?limit=100')
users_data = users_response.json()
users = users_data['users']

# Helper function to generate random dates
def random_date(start_date='2024-01-01', end_date='2024-12-31'):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    random_days = random.randint(0, (end - start).days)
    return start + timedelta(days=random_days)

# Generate CSV sales data (33 records)
csv_data = []
order_counter = 102001
line_counter = 1
records_needed = 33

while records_needed > 0:
    # Create order details
    order_id = f"ORD{order_counter}"
    order_date = random_date()
    delivery_date = order_date + timedelta(days=random.randint(2, 10))
    customer = random.choice(users)
    customer_id = f"CUS{str(customer['id']).zfill(3)}"
    store_id = f"STORE{str(random.randint(1, 10)).zfill(2)}"

    # Add 1-4 items per order
    items_in_order = min(random.randint(1, 4), records_needed)
    for item_num in range(1, items_in_order + 1):
        product = random.choice(products)
        product_id = f"PRD{str(product['id']).zfill(3)}"
        quantity = random.randint(1, 5)

        csv_data.append([
            order_id,
            item_num,
            order_date.strftime('%Y-%m-%d'),
            delivery_date.strftime('%Y-%m-%d'),
            customer_id,
            store_id,
            product_id,
            quantity,
            "USD"
        ])
        records_needed -= 1

    order_counter += 1

# Write CSV file
with open('sales.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerows(csv_data)

# Generate JSON sales data (48 records)
json_sales = []
payment_methods = ['credit_card', 'debit_card', 'paypal', 'cash_on_delivery']

for i in range(1, 49):
    order_id = f"ORD{1082 + i}"
    order_date = random_date().strftime('%Y-%m-%d')
    customer = random.choice(users)

    # Create items (1-4 per order)
    items = []
    total_amount = 0.0
    for _ in range(random.randint(1, 4)):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        unit_price = product['price']
        total_price = round(quantity * unit_price, 2)

        items.append({
            "product_id": f"PRD{str(product['id']).zfill(3)}",
            "product_name": product['title'],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price
        })
        total_amount += total_price

    # Create shipping address
    address = customer['address']
    shipping_address = {
        "street": address.get('address', ''),
        "city": address.get('city', ''),
        "state": address.get('stateCode', address.get('state', '')),
        "zip": address.get('postalCode', '')
    }

    json_sales.append({
        "order_id": order_id,
        "order_date": order_date,
        "customer": {
            "id": f"CUS{str(customer['id']).zfill(3)}",
            "name": f"{customer['firstName']} {customer['lastName']}"
        },
        "items": items,
        "total_amount": round(total_amount, 2),
        "currency": "USD",
        "payment_method": random.choice(payment_methods),
        "shipping_address": shipping_address
    })

# Write JSON file
with open('sales.json', 'w') as json_file:
    json.dump({"sales": json_sales}, json_file, indent=2)

print("Data generation completed!")
print(f"CSV records: {len(csv_data)}")
print(f"JSON records: {len(json_sales)}")
