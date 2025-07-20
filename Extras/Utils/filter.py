import json
import re
import csv

# Filter sales.json
with open('data/sales.json', 'r') as f:
    data = json.load(f)

filtered_sales = []
for sale in data['sales']:
    cust_id = sale['customer']['id']
    match = re.match(r'^CUS(\d+)', cust_id)
    if match and int(match.group(1)) > 30:
        continue
    # Remove items with product_id starting with PRD and number > 30
    filtered_items = []
    for item in sale['items']:
        match = re.match(r'^PRD(\d+)', item['product_id'])
        if match and int(match.group(1)) > 30:
            continue
        filtered_items.append(item)
    sale['items'] = filtered_items
    sale['total_amount'] = round(sum(item['total_price'] for item in filtered_items), 2)
    filtered_sales.append(sale)
data['sales'] = filtered_sales

with open('data/sales.json', 'w') as f:
    json.dump(data, f, indent=2)

# Filter sales.csv
filtered_rows = []
with open('data/sales.csv', 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    fieldnames = reader.fieldnames
    for row in reader:
        match = re.match(r'^PRD(\d+)', row['product_key'])
        if match and int(match.group(1)) > 30:
            continue
        filtered_rows.append(row)

with open('data/sales.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(filtered_rows)
