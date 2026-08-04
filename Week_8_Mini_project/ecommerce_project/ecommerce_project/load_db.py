"""
Loads the cleaned CSV files into a SQLite database (db/ecommerce.db)
so that Part 3 (SQL Analysis) and Part 4 (CLI tool) can query it.

Also excludes the known "orphan" order_items rows (those that fail
referential integrity) before loading into order_items, and creates
indexes to keep the window-function queries fast.
"""

import sqlite3
import pandas as pd
import os

CLEAN_DIR = "cleaned_data"
DB_PATH = "db/ecommerce.db"


def load():
    os.makedirs("db", exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)

    customers = pd.read_csv(f"{CLEAN_DIR}/customers.csv")
    products = pd.read_csv(f"{CLEAN_DIR}/products.csv")
    orders = pd.read_csv(f"{CLEAN_DIR}/orders.csv")
    order_items = pd.read_csv(f"{CLEAN_DIR}/order_items.csv")

    # orders.csv customer_id may contain NaN (guest orders) -> fine for SQLite
    # order_date was saved as pandas datetime string by clean_orders(); keep as text (ISO) for SQLite
    orders["order_date"] = pd.to_datetime(orders["order_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Drop order_items rows that reference non-existent orders (orphans),
    # matching what check_referential_integrity() flagged in Part 2.
    valid_order_ids = set(orders["order_id"])
    before = len(order_items)
    order_items = order_items[order_items["order_id"].isin(valid_order_ids)]
    dropped = before - len(order_items)

    customers.to_sql("customers", conn, if_exists="replace", index=False)
    products.to_sql("products", conn, if_exists="replace", index=False)
    orders.to_sql("orders", conn, if_exists="replace", index=False)
    order_items.to_sql("order_items", conn, if_exists="replace", index=False)

    cur = conn.cursor()
    cur.executescript("""
        CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
        CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
        CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id);
        CREATE INDEX IF NOT EXISTS idx_items_product ON order_items(product_id);
        CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
    """)
    conn.commit()
    conn.close()

    print(f"Loaded into {DB_PATH}:")
    print(f"  customers:   {len(customers)} rows")
    print(f"  products:    {len(products)} rows")
    print(f"  orders:      {len(orders)} rows")
    print(f"  order_items: {len(order_items)} rows ({dropped} orphan rows dropped)")


if __name__ == "__main__":
    load()
