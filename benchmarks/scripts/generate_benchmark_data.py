#!/usr/bin/env python3
"""Generate benchmark CSV data with 2 million rows."""

import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_ROWS = 2_000_000
OUTPUT_FILE = "benchmarks/data/benchmark_data.csv"

# Sample data pools
PRODUCTS = ["Laptop", "Mouse", "Keyboard", "Monitor", "Desk", "Chair", "Headset", "Webcam", "Speaker", "Tablet"]
REGIONS = ["North", "South", "East", "West", "Central"]
STATUSES = ["shipped", "pending", "processing", "cancelled", "delivered"]
FIRST_NAMES = ["John", "Sarah", "Mike", "Emily", "David", "Jennifer", "Robert", "Lisa", "James", "Mary",
               "Michael", "Linda", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Karen"]
LAST_NAMES = ["Smith", "Johnson", "Brown", "Davis", "Wilson", "Lee", "Taylor", "Anderson", "Thomas", "Jackson",
              "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis"]

# Price ranges for products
PRICES = {
    "Laptop": (599.99, 1499.99),
    "Mouse": (9.99, 79.99),
    "Keyboard": (19.99, 199.99),
    "Monitor": (149.99, 899.99),
    "Desk": (199.99, 799.99),
    "Chair": (99.99, 599.99),
    "Headset": (29.99, 299.99),
    "Webcam": (39.99, 199.99),
    "Speaker": (49.99, 399.99),
    "Tablet": (299.99, 1099.99),
}

def generate_row(order_id):
    """Generate a single row of data."""
    product = random.choice(PRODUCTS)
    price_range = PRICES[product]

    return {
        "order_id": order_id,
        "customer_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        "product": product,
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(price_range[0], price_range[1]), 2),
        "order_date": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "region": random.choice(REGIONS),
        "status": random.choice(STATUSES),
    }

def main():
    print(f"Generating {NUM_ROWS:,} rows of benchmark data...")
    print(f"Output file: {OUTPUT_FILE}")

    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        fieldnames = ["order_id", "customer_name", "product", "quantity", "price", "order_date", "region", "status"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Write data rows
        for i in range(1, NUM_ROWS + 1):
            writer.writerow(generate_row(1000 + i))

            # Progress indicator
            if i % 100000 == 0:
                print(f"  Written {i:,} rows ({i/NUM_ROWS*100:.1f}%)")

    print(f"✓ Generated {NUM_ROWS:,} rows successfully!")

    # Print file size
    import os
    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"✓ File size: {file_size / (1024*1024):.2f} MB")

if __name__ == "__main__":
    main()
