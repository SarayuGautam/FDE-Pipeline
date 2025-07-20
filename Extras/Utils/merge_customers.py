import json
import csv
from collections import OrderedDict

# Load sales data
with open('data/sales.json', 'r') as f:
    sales_data = json.load(f)

# Load customer data and create mapping
customer_map = OrderedDict()
with open('data/customers.csv', 'r') as f:
    reader = csv.DictReader(f)
    for idx, row in enumerate(reader):
        if idx < 8:  # We only need 8 distinct customers
            customer_map[row['customer_key']] = {
                'id': row['customer_key'],
                'name': row['name']
            }

# Map of original customer IDs to valid customer keys
original_to_valid = {
    'CUST101': 'CUST001',
    'CUST205': 'CUST002',
    'CUST342': 'CUST003',
    'COST078': 'CUST004',
    'CUST512': 'CUST005',
    'CUST623': 'CUST006',
    'CUST789': 'CUST007',
    'CUST890': 'CUST008'
}

# Update sales data with valid customer information
for order in sales_data['sales']:
    original_id = order['customer']['id']
    if original_id in original_to_valid:
        valid_key = original_to_valid[original_id]
        order['customer'] = customer_map[valid_key]

# Save updated sales data
with open('updated_sales.json', 'w') as f:
    json.dump(sales_data, f, indent=2)

print("Sales data updated successfully. Output saved to 'updated_sales.json'")
