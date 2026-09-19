"""
SparkRetail - synthetic dataset generator

Produces three CSVs that mirror the synopsis:
  1. sales.csv     - the main transactional fact table (deliberately skewed
                     towards one region, per Module 2's "data skew" study)
  2. customers.csv - small dimension table, joined in Module 2
  3. regions.csv   - tiny lookup table, used for the BROADCAST join in Module 2

Run: python generate_data.py
"""

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(OUT_DIR, exist_ok=True)

# ---- Dimension data -------------------------------------------------

REGIONS = ["North", "South", "East", "West", "Central"]
# Deliberate skew: 'North' gets ~65% of all transactions so Module 2 has a
# real data-skew problem to demonstrate (and fix with broadcast join + salting).
REGION_WEIGHTS = [0.65, 0.10, 0.10, 0.08, 0.07]

STORES = {
    "North": ["STR-N1", "STR-N2"],
    "South": ["STR-S1"],
    "East": ["STR-E1"],
    "West": ["STR-W1"],
    "Central": ["STR-C1"],
}

PRODUCTS = [
    ("P001", "Wireless Mouse", 599),
    ("P002", "Mechanical Keyboard", 2499),
    ("P003", "USB-C Cable", 199),
    ("P004", "Laptop Stand", 1299),
    ("P005", "Bluetooth Speaker", 1799),
    ("P006", "Webcam HD", 2199),
    ("P007", "Notebook Set", 149),
    ("P008", "Desk Lamp", 899),
    ("P009", "Backpack", 1599),
    ("P010", "Power Bank", 999),
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Cash", "Net Banking"]

N_CUSTOMERS = 2000
N_TRANSACTIONS = 300_000  # large enough to make Spark's distributed advantage visible

# ---- regions.csv (tiny lookup -> broadcast join target) --------------

with open(f"{OUT_DIR}/regions.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["region", "region_manager", "region_target_revenue"])
    for r in REGIONS:
        w.writerow([r, f"{r} Manager", random.randint(500000, 2000000)])

# ---- customers.csv (small dimension table) ---------------------------

FIRST_NAMES = ["Aarav", "Vivaan", "Aditi", "Isha", "Rohan", "Sneha", "Kabir",
               "Meera", "Arjun", "Diya", "Yash", "Priya", "Karan", "Anaya"]
LAST_NAMES = ["Shah", "Verma", "Iyer", "Khan", "Nair", "Gupta", "Reddy",
              "Joshi", "Pillai", "Mehta"]

with open(f"{OUT_DIR}/customers.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "customer_name", "signup_date"])
    for cid in range(1, N_CUSTOMERS + 1):
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        signup = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 600))
        w.writerow([f"CUST-{cid:05d}", name, signup.strftime("%Y-%m-%d")])

# ---- sales.csv (fact table - the big, skewed one) --------------------

start_date = datetime(2025, 1, 1)

with open(f"{OUT_DIR}/sales.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow([
        "order_id", "order_date", "customer_id", "region", "store_id",
        "product_id", "product_name", "unit_price", "quantity",
        "discount_pct", "payment_method",
    ])
    for i in range(1, N_TRANSACTIONS + 1):
        region = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]
        store_id = random.choice(STORES[region])
        product_id, product_name, unit_price = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)
        discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])
        order_date = start_date + timedelta(minutes=random.randint(0, 60 * 24 * 270))
        w.writerow([
            f"ORD-{i:07d}",
            order_date.strftime("%Y-%m-%d %H:%M:%S"),
            f"CUST-{random.randint(1, N_CUSTOMERS):05d}",
            region,
            store_id,
            product_id,
            product_name,
            unit_price,
            quantity,
            discount_pct,
            random.choice(PAYMENT_METHODS),
        ])

print(f"Done. Wrote {N_TRANSACTIONS:,} sales rows, {N_CUSTOMERS} customers, "
      f"{len(REGIONS)} regions to {OUT_DIR}/")
print("Region distribution target (for the skew study):",
      dict(zip(REGIONS, REGION_WEIGHTS)))
