"""
Generates the full synthetic dataset package for the Retail E-commerce
Sales Analytics Pipeline intern project (Celebal Technologies CEI assignment).

Produces, under ./datasets/:
  batch/orders_batch.csv
  batch/customers_batch.csv
  batch/products_batch.csv
  batch/stores_batch.csv
  incremental/day_YYYY-MM-DD/orders_incremental_YYYY-MM-DD.csv
  incremental/day_YYYY-MM-DD/customers_cdc_YYYY-MM-DD.csv
  incremental/day_YYYY-MM-DD/products_cdc_YYYY-MM-DD.csv

Deliberately injects the messiness the guide calls out:
  - null customer/product IDs
  - duplicate orders (exact dupes + "updated" dupes with newer values)
  - invalid / malformed dates
  - text inside numeric columns (currency symbols, "unknown", junk strings)
  - updated customer/product attributes (feeds SCD2 on the CDC files)
  - late-arriving orders (order_date earlier than the file's "as-of" day)
  - a schema change on day 2026-04-26 (new coupon_code column in orders)
"""
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)

BASE = os.path.dirname(os.path.abspath(__file__))
BATCH_DIR = os.path.join(BASE, "datasets", "batch")
INCR_DIR = os.path.join(BASE, "datasets", "incremental")
os.makedirs(BATCH_DIR, exist_ok=True)
os.makedirs(INCR_DIR, exist_ok=True)

FIRST_NAMES = ["Aarav", "Vivaan", "Aditya", "Priya", "Ananya", "Isha", "Rohan",
               "Kabir", "Meera", "Sara", "Arjun", "Diya", "Kavya", "Nikhil",
               "Riya", "Yash", "Zara", "Ishaan", "Tara", "Dev"]
LAST_NAMES = ["Sharma", "Verma", "Patel", "Iyer", "Reddy", "Nair", "Gupta",
              "Singh", "Rao", "Mehta", "Kapoor", "Joshi", "Das", "Chatterjee"]
CITIES = [("Mumbai", "West"), ("Delhi", "North"), ("Bengaluru", "South"),
          ("Chennai", "South"), ("Kolkata", "East"), ("Pune", "West"),
          ("Hyderabad", "South"), ("Ahmedabad", "West"), ("Jaipur", "North"),
          ("Lucknow", "North"), ("Bhubaneswar", "East"), ("Guwahati", "East")]
SEGMENTS = ["Consumer", "Corporate", "Home Office"]
CATEGORIES = {
    "Electronics": ["Wireless Mouse", "Bluetooth Speaker", "USB-C Charger",
                    "Smartwatch", "Laptop Stand", "Noise Cancelling Headphones"],
    "Apparel": ["Cotton T-Shirt", "Denim Jacket", "Running Shoes", "Wool Sweater",
                "Rain Jacket", "Formal Shirt"],
    "Home & Kitchen": ["Non-stick Pan", "Air Fryer", "Coffee Maker",
                        "Bedsheet Set", "Table Lamp", "Vacuum Cleaner"],
    "Books": ["Fiction Novel", "Cookbook", "Self-Help Guide", "Biography",
              "Children's Storybook", "Graphic Novel"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Cricket Bat", "Football",
               "Cycling Helmet", "Resistance Bands"],
}
STORE_REGIONS = ["North", "South", "East", "West"]

CUSTOMER_COUNT = 1500
PRODUCT_COUNT = 300
STORE_COUNT = 25
BATCH_ORDER_COUNT = 12500

BATCH_START = datetime(2025, 1, 1)
BATCH_END = datetime(2026, 4, 22)
INCR_DAYS = ["2026-04-23", "2026-04-24", "2026-04-25", "2026-04-26", "2026-04-27"]
SCHEMA_CHANGE_DAY = "2026-04-26"


def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def maybe(value, none_rate=0.05):
    return "" if random.random() < none_rate else value


# ---------------------------------------------------------------- customers
def gen_customers():
    rows = []
    ids = []
    for i in range(1, CUSTOMER_COUNT + 1):
        cid = f"CUST{i:05d}"
        ids.append(cid)
        city, region = random.choice(CITIES)
        signup = rand_date(datetime(2022, 1, 1), BATCH_END)
        rows.append({
            "customer_id": cid,
            "customer_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "email": f"cust{i}@example.com",
            "city": maybe(city, 0.04),
            "segment": maybe(random.choice(SEGMENTS), 0.03),
            "signup_date": signup.strftime("%Y-%m-%d"),
        })

    # invalid signup dates
    for r in random.sample(rows, 40):
        r["signup_date"] = random.choice(["0000-00-00", "31/02/2025", "not_a_date", ""])

    # null customer_id records
    for _ in range(25):
        bad = dict(random.choice(rows))
        bad["customer_id"] = ""
        rows.append(bad)

    # exact duplicates
    for r in random.sample(rows[:CUSTOMER_COUNT], 60):
        rows.append(dict(r))

    random.shuffle(rows)
    return rows, ids


def write_customers(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["customer_id", "customer_name", "email",
                                           "city", "segment", "signup_date"])
        w.writeheader()
        w.writerows(rows)


# ----------------------------------------------------------------- products
def gen_products():
    rows = []
    ids = []
    pid = 1
    for category, items in CATEGORIES.items():
        for _ in range(PRODUCT_COUNT // len(CATEGORIES)):
            p = f"PROD{pid:05d}"
            ids.append(p)
            item = random.choice(items)
            price = round(random.uniform(150, 25000), 2)
            rows.append({
                "product_id": p,
                "product_name": f"{item} {random.choice(['Pro','Plus','Lite','Max',''])}".strip(),
                "category": category,
                "unit_price": price,
            })
            pid += 1

    # price anomalies: text / currency symbols / "unknown"
    for r in random.sample(rows, 45):
        r["unit_price"] = random.choice(["unknown", "N/A", "$" + str(r["unit_price"]),
                                          "Rs." + str(r["unit_price"]), "-1", ""])

    # null product_id
    for _ in range(20):
        bad = dict(random.choice(rows))
        bad["product_id"] = ""
        rows.append(bad)

    # duplicates
    for r in random.sample(rows[:len(ids)], 30):
        rows.append(dict(r))

    random.shuffle(rows)
    return rows, ids


def write_products(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["product_id", "product_name", "category", "unit_price"])
        w.writeheader()
        w.writerows(rows)


# ------------------------------------------------------------------- stores
def gen_stores():
    rows = []
    ids = []
    for i in range(1, STORE_COUNT + 1):
        sid = f"STORE{i:03d}"
        ids.append(sid)
        city, region = random.choice(CITIES)
        rows.append({
            "store_id": sid,
            "store_name": f"{city} Store {i}",
            "city": city,
            "region": region,
        })
    for r in random.sample(rows, 5):
        rows.append(dict(r))  # duplicates
    random.shuffle(rows)
    return rows, ids


def write_stores(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["store_id", "store_name", "city", "region"])
        w.writeheader()
        w.writerows(rows)


# ------------------------------------------------------------------- orders
def gen_orders(order_start_num, count, customer_ids, product_ids, store_ids,
                date_start, date_end, corrupt_rate=0.05, coupon_col=False,
                late_rate=0.0):
    rows = []
    oid = order_start_num
    for _ in range(count):
        order_id = f"ORD{oid:07d}"
        oid += 1
        cust = random.choice(customer_ids)
        prod = random.choice(product_ids)
        store = random.choice(store_ids)
        qty = random.randint(1, 6)
        base_price = round(random.uniform(150, 25000), 2)
        odate = rand_date(date_start, date_end)
        if late_rate and random.random() < late_rate:
            odate = odate - timedelta(days=random.randint(3, 10))

        row = {
            "order_id": order_id,
            "customer_id": cust,
            "product_id": prod,
            "store_id": store,
            "order_date": odate.strftime("%Y-%m-%d"),
            "quantity": qty,
            "unit_price": base_price,
        }
        if coupon_col:
            row["coupon_code"] = random.choice(["", "", "", "SAVE10", "FEST50", "WELCOME5"])
        rows.append(row)

    # corrupt a slice: nulls, bad dates, text-in-numeric
    corrupt_n = int(count * corrupt_rate)
    for r in random.sample(rows, corrupt_n):
        choice = random.random()
        if choice < 0.25:
            r["customer_id"] = ""
        elif choice < 0.45:
            r["product_id"] = ""
        elif choice < 0.65:
            r["order_date"] = random.choice(["2026-13-40", "not_a_date", "", "00/00/0000"])
        elif choice < 0.85:
            r["unit_price"] = random.choice(["$" + str(r["unit_price"]), "unknown", "-5", "abc"])
        else:
            r["quantity"] = random.choice(["", "two", "-1"])

    # duplicate rows (exact copies)
    dup_n = max(1, int(count * 0.03))
    for r in random.sample(rows, dup_n):
        rows.append(dict(r))

    random.shuffle(rows)
    return rows, oid


def write_orders(rows, path, coupon_col=False):
    fields = ["order_id", "customer_id", "product_id", "store_id", "order_date",
              "quantity", "unit_price"]
    if coupon_col:
        fields.append("coupon_code")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


# --------------------------------------------------------------------- CDC
def gen_customer_cdc(customer_ids, n, as_of_date):
    rows = []
    sample = random.sample(customer_ids, min(n, len(customer_ids)))
    for cid in sample:
        city, region = random.choice(CITIES)
        rows.append({
            "customer_id": cid,
            "customer_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "email": f"{cid.lower()}@example.com",
            "city": city,
            "segment": random.choice(SEGMENTS),
            "change_date": as_of_date,
        })
    return rows


def write_customer_cdc(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["customer_id", "customer_name", "email",
                                           "city", "segment", "change_date"])
        w.writeheader()
        w.writerows(rows)


def gen_product_cdc(product_ids, n, as_of_date):
    rows = []
    sample = random.sample(product_ids, min(n, len(product_ids)))
    for pid in sample:
        category = random.choice(list(CATEGORIES.keys()))
        rows.append({
            "product_id": pid,
            "product_name": random.choice(CATEGORIES[category]),
            "category": category,
            "unit_price": round(random.uniform(150, 25000), 2),
            "change_date": as_of_date,
        })
    return rows


def write_product_cdc(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["product_id", "product_name", "category",
                                           "unit_price", "change_date"])
        w.writeheader()
        w.writerows(rows)


def main():
    customers, customer_ids = gen_customers()
    write_customers(customers, os.path.join(BATCH_DIR, "customers_batch.csv"))

    products, product_ids = gen_products()
    write_products(products, os.path.join(BATCH_DIR, "products_batch.csv"))

    stores, store_ids = gen_stores()
    write_stores(stores, os.path.join(BATCH_DIR, "stores_batch.csv"))

    orders, next_oid = gen_orders(1, BATCH_ORDER_COUNT, customer_ids, product_ids,
                                    store_ids, BATCH_START, BATCH_END,
                                    corrupt_rate=0.06)
    write_orders(orders, os.path.join(BATCH_DIR, "orders_batch.csv"))

    print(f"batch: {len(customers)} customer rows, {len(products)} product rows, "
          f"{len(stores)} store rows, {len(orders)} order rows")

    for day in INCR_DAYS:
        day_dir = os.path.join(INCR_DIR, f"day_{day}")
        os.makedirs(day_dir, exist_ok=True)
        day_dt = datetime.strptime(day, "%Y-%m-%d")

        coupon = (day == SCHEMA_CHANGE_DAY)
        day_orders, next_oid = gen_orders(
            next_oid, 2000, customer_ids, product_ids, store_ids,
            day_dt, day_dt, corrupt_rate=0.05, coupon_col=coupon, late_rate=0.08,
        )
        write_orders(day_orders,
                     os.path.join(day_dir, f"orders_incremental_{day}.csv"),
                     coupon_col=coupon)

        cust_cdc = gen_customer_cdc(customer_ids, 60, day)
        write_customer_cdc(cust_cdc, os.path.join(day_dir, f"customers_cdc_{day}.csv"))

        prod_cdc = gen_product_cdc(product_ids, 40, day)
        write_product_cdc(prod_cdc, os.path.join(day_dir, f"products_cdc_{day}.csv"))

        print(f"day {day}: {len(day_orders)} orders "
              f"{'(with coupon_code — schema change)' if coupon else ''}, "
              f"{len(cust_cdc)} customer CDC, {len(prod_cdc)} product CDC")


if __name__ == "__main__":
    main()
