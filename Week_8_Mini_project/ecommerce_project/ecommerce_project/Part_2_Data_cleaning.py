"""
Part 2: Data Cleaning
----------------------
Implements the 4 required functions:
    1. clean_orders()               - fix date formats, handle NULL customer_ids
    2. clean_products()             - normalize product names (trim spaces, title case)
    3. validate_emails()            - return list of customer_ids with invalid emails
    4. check_referential_integrity() - find order_items that reference non-existent orders

Running this file as a script will:
    - read the raw CSVs from ./data/
    - clean them
    - write cleaned CSVs to ./cleaned_data/
    - write a full issues report to ./reports/data_quality_report.txt
"""

import re
import pandas as pd

RAW_DIR = "data"
CLEAN_DIR = "cleaned_data"
REPORT_PATH = "reports/data_quality_report.txt"

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------------------------------------------------------------------------
# 1. clean_orders()
# ---------------------------------------------------------------------------

def clean_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Fixes:
      - order_date: some rows are in DD-MM-YYYY format instead of
        YYYY-MM-DD HH:MM:SS. We detect and reparse those, defaulting the
        missing time component to 00:00:00.
      - customer_id: empty/NULL values are kept as proper pandas NA (instead
        of empty string), and counted for the report. We do NOT drop these
        rows (a guest/anonymous order is still a valid order for revenue
        reporting) but flag them clearly.

    Returns the cleaned dataframe plus a small stats dict for the report.
    """
    df = df.copy()
    stats = {}

    # --- fix order_date ---
    def parse_date(value):
        if pd.isna(value) or str(value).strip() == "":
            return pd.NaT
        value = str(value).strip()
        # Try the correct format first
        for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%m-%Y"):
            try:
                return pd.to_datetime(value, format=fmt)
            except ValueError:
                continue
        # Fallback: let pandas guess
        return pd.to_datetime(value, errors="coerce")

    original_dates = df["order_date"]
    parsed = original_dates.apply(parse_date)
    n_wrong_format = int(
        original_dates.apply(lambda v: bool(re.match(r"^\d{2}-\d{2}-\d{4}$", str(v).strip())))
        .sum()
    )
    n_unparseable = int(parsed.isna().sum())
    df["order_date"] = parsed

    stats["wrong_format_dates_fixed"] = n_wrong_format
    stats["unparseable_dates"] = n_unparseable

    # --- handle NULL customer_id ---
    df["customer_id"] = df["customer_id"].replace("", pd.NA)
    n_null_customers = int(df["customer_id"].isna().sum())
    stats["null_customer_ids"] = n_null_customers

    # keep a clean nullable integer type
    df["customer_id"] = pd.array(df["customer_id"], dtype="Int64")

    return df, stats


# ---------------------------------------------------------------------------
# 2. clean_products()
# ---------------------------------------------------------------------------

def clean_products(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Normalizes product_name: trims leading/trailing whitespace, collapses
    internal repeated whitespace, and converts to Title Case so that
    "  wireless earbuds  ", "WIRELESS EARBUDS", and "Wireless Earbuds" all
    become the same canonical "Wireless Earbuds".
    """
    df = df.copy()
    stats = {}

    original_names = df["product_name"].copy()

    def normalize(name):
        if pd.isna(name):
            return name
        cleaned = re.sub(r"\s+", " ", str(name).strip())
        return cleaned.title()

    df["product_name"] = df["product_name"].apply(normalize)

    n_changed = int((df["product_name"] != original_names).sum())
    stats["product_names_normalized"] = n_changed

    return df, stats


# ---------------------------------------------------------------------------
# 3. validate_emails()
# ---------------------------------------------------------------------------

def validate_emails(df: pd.DataFrame) -> list:
    """
    Returns a list of customer_ids whose email is invalid, i.e. missing
    '@', missing a domain, or otherwise not matching a basic email pattern.
    """
    invalid_ids = []
    for _, row in df.iterrows():
        email = row.get("email")
        cust_id = row.get("customer_id")
        if pd.isna(email) or not EMAIL_REGEX.match(str(email).strip()):
            invalid_ids.append(cust_id)
    return invalid_ids


# ---------------------------------------------------------------------------
# 4. check_referential_integrity()
# ---------------------------------------------------------------------------

def check_referential_integrity(orders_df: pd.DataFrame, order_items_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the subset of order_items rows whose order_id does NOT exist
    in orders_df. These are "orphan" rows caused by an upstream pipeline
    issue and should be reported (and typically excluded from SQL analysis).
    """
    valid_order_ids = set(orders_df["order_id"])
    mask = ~order_items_df["order_id"].isin(valid_order_ids)
    return order_items_df[mask]


# ---------------------------------------------------------------------------
# Orchestration: run all cleaning steps, write cleaned CSVs + report
# ---------------------------------------------------------------------------

def run_all():
    import os
    os.makedirs(CLEAN_DIR, exist_ok=True)
    os.makedirs("reports", exist_ok=True)

    orders_raw = pd.read_csv(f"{RAW_DIR}/orders.csv", dtype={"customer_id": "object"})
    products_raw = pd.read_csv(f"{RAW_DIR}/products.csv")
    customers_raw = pd.read_csv(f"{RAW_DIR}/customers.csv")
    order_items_raw = pd.read_csv(f"{RAW_DIR}/order_items.csv")

    orders_clean, order_stats = clean_orders(orders_raw)
    products_clean, product_stats = clean_products(products_raw)
    invalid_email_ids = validate_emails(customers_raw)
    orphan_items = check_referential_integrity(orders_raw, order_items_raw)

    # customers.csv and order_items.csv pass through largely unchanged,
    # but we still write them to cleaned_data/ so downstream steps (SQL
    # loading) read from a single consistent folder.
    customers_clean = customers_raw.copy()
    order_items_clean = order_items_raw.copy()

    # Extra cleaning per assignment spirit: discount_percent should be
    # clipped to a valid 0-100 range, negative quantities are legitimate
    # (returns) and are left as-is per the spec.
    n_bad_discount = int(((order_items_clean["discount_percent"] < 0) |
                           (order_items_clean["discount_percent"] > 100)).sum())
    order_items_clean["discount_percent"] = order_items_clean["discount_percent"].clip(0, 100)

    n_returns = int((order_items_clean["quantity"] < 0).sum())
    n_zero_qty = int((order_items_clean["quantity"] == 0).sum())

    # write cleaned CSVs
    orders_clean.to_csv(f"{CLEAN_DIR}/orders.csv", index=False)
    products_clean.to_csv(f"{CLEAN_DIR}/products.csv", index=False)
    customers_clean.to_csv(f"{CLEAN_DIR}/customers.csv", index=False)
    order_items_clean.to_csv(f"{CLEAN_DIR}/order_items.csv", index=False)

    # --- build report ---
    lines = []
    lines.append("DATA QUALITY REPORT")
    lines.append("=" * 60)
    lines.append(f"Rows processed: orders={len(orders_raw)}, order_items={len(order_items_raw)}, "
                  f"products={len(products_raw)}, customers={len(customers_raw)}")
    lines.append("")
    lines.append("-- clean_orders() --")
    lines.append(f"  Orders with wrong date format (DD-MM-YYYY) fixed: {order_stats['wrong_format_dates_fixed']}")
    lines.append(f"  Orders with unparseable order_date: {order_stats['unparseable_dates']}")
    lines.append(f"  Orders with NULL/missing customer_id: {order_stats['null_customer_ids']}")
    lines.append("")
    lines.append("-- clean_products() --")
    lines.append(f"  Product names normalized (case/whitespace fixed): {product_stats['product_names_normalized']}")
    lines.append("")
    lines.append("-- validate_emails() --")
    lines.append(f"  Customers with invalid email: {len(invalid_email_ids)}")
    lines.append(f"  Invalid customer_ids: {invalid_email_ids[:20]}"
                  f"{' ...(truncated)' if len(invalid_email_ids) > 20 else ''}")
    lines.append("")
    lines.append("-- check_referential_integrity() --")
    lines.append(f"  order_items rows referencing non-existent orders: {len(orphan_items)}")
    lines.append(f"  Orphan order_ids referenced: {sorted(orphan_items['order_id'].unique().tolist())}")
    lines.append("")
    lines.append("-- order_items sanity checks --")
    lines.append(f"  Rows with discount_percent outside 0-100 (clipped): {n_bad_discount}")
    lines.append(f"  Rows with negative quantity (returns, kept as-is): {n_returns}")
    lines.append(f"  Rows with zero quantity: {n_zero_qty}")

    report_text = "\n".join(lines)
    with open(REPORT_PATH, "w") as f:
        f.write(report_text)

    print(report_text)
    print(f"\nCleaned CSVs written to ./{CLEAN_DIR}/")
    print(f"Report written to ./{REPORT_PATH}")


if __name__ == "__main__":
    run_all()
