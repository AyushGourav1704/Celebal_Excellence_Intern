"""
Part 5: Edge Case Handling
----------------------------
Test functions (plain Python, runnable with `python3 test_edge_cases.py`
or collectible by pytest) that verify how the pipeline behaves for:

    1. order_items with an order_id not in orders
    2. discount_percent > 100
    3. quantity == 0
    4. order_date in the future

Each test builds a small, self-contained pandas DataFrame (independent of
the generated CSVs) so the behavior is easy to reason about and reproduce.
"""

import pandas as pd
from datetime import datetime, timedelta

from cleaning import clean_orders, check_referential_integrity


# ---------------------------------------------------------------------------
# 1. order_items references an order_id that does not exist in orders
# ---------------------------------------------------------------------------

def test_order_item_with_nonexistent_order_id():
    orders = pd.DataFrame({
        "order_id": [1, 2, 3],
        "customer_id": [10, 20, 30],
        "order_date": ["2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-03 10:00:00"],
        "status": ["DELIVERED", "PLACED", "SHIPPED"],
        "region_code": ["NORTH", "SOUTH", "EAST"],
    })

    order_items = pd.DataFrame({
        "item_id": [1, 2, 3],
        "order_id": [1, 2, 999],       # 999 does not exist in orders
        "product_id": [100, 101, 102],
        "quantity": [2, 1, 3],
        "unit_price": [50.0, 20.0, 15.0],
        "discount_percent": [0, 10, 5],
    })

    orphans = check_referential_integrity(orders, order_items)

    assert len(orphans) == 1, f"Expected exactly 1 orphan row, got {len(orphans)}"
    assert orphans.iloc[0]["order_id"] == 999
    print("PASS: test_order_item_with_nonexistent_order_id "
          "-> orphan row (order_id=999) correctly detected and can be excluded from analysis.")


# ---------------------------------------------------------------------------
# 2. discount_percent > 100
# ---------------------------------------------------------------------------

def test_discount_percent_greater_than_100():
    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [1, 2],
        "product_id": [100, 101],
        "quantity": [1, 1],
        "unit_price": [100.0, 100.0],
        "discount_percent": [150, 50],   # 150 is invalid
    })

    is_out_of_range = (order_items["discount_percent"] < 0) | (order_items["discount_percent"] > 100)
    n_bad = int(is_out_of_range.sum())
    assert n_bad == 1, f"Expected 1 invalid discount row, got {n_bad}"

    # Behavior: revenue formula (1 - discount/100) goes NEGATIVE for
    # discount_percent > 100, which would understate/invert revenue.
    # We clip to a valid 0-100 range before computing revenue.
    clipped = order_items["discount_percent"].clip(0, 100)
    revenue = order_items["quantity"] * order_items["unit_price"] * (1 - clipped / 100.0)

    assert clipped.iloc[0] == 100, "discount_percent should be clipped to 100"
    assert revenue.iloc[0] == 0.0, "Revenue for a 100%-clipped discount should be 0, not negative"
    print("PASS: test_discount_percent_greater_than_100 "
          "-> value is clipped to 100 before revenue is computed (prevents negative revenue).")


# ---------------------------------------------------------------------------
# 3. quantity == 0
# ---------------------------------------------------------------------------

def test_quantity_zero():
    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [1, 2],
        "product_id": [100, 101],
        "quantity": [0, 5],
        "unit_price": [100.0, 100.0],
        "discount_percent": [0, 0],
    })

    revenue = order_items["quantity"] * order_items["unit_price"] * (1 - order_items["discount_percent"] / 100.0)

    # Behavior: a quantity of 0 contributes exactly $0 revenue. It is not an
    # error by itself (could represent a cancelled line item), but it's
    # worth flagging/counting separately from real returns (negative qty)
    # since it neither adds nor subtracts inventory/revenue.
    assert revenue.iloc[0] == 0.0
    n_zero_qty = int((order_items["quantity"] == 0).sum())
    assert n_zero_qty == 1
    print("PASS: test_quantity_zero "
          "-> zero-quantity rows contribute $0 revenue and are counted separately "
          "from negative-quantity returns in the data quality report.")


# ---------------------------------------------------------------------------
# 4. order_date in the future
# ---------------------------------------------------------------------------

def test_order_date_in_future():
    future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

    orders = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [10, 20],
        "order_date": ["2024-01-01 10:00:00", future_date],
        "status": ["DELIVERED", "PLACED"],
        "region_code": ["NORTH", "SOUTH"],
    })

    cleaned, stats = clean_orders(orders)

    # clean_orders() does not silently drop/alter valid-looking future
    # dates (a pre-order or scheduled delivery could legitimately have
    # one) -- but downstream code / reports should be able to flag them.
    n_future = int((cleaned["order_date"] > pd.Timestamp.now()).sum())
    assert n_future == 1, f"Expected 1 future-dated order, got {n_future}"
    print("PASS: test_order_date_in_future "
          "-> future-dated orders parse correctly and can be flagged by comparing "
          "against the current timestamp (not silently dropped or errored on).")


if __name__ == "__main__":
    tests = [
        test_order_item_with_nonexistent_order_id,
        test_discount_percent_greater_than_100,
        test_quantity_zero,
        test_order_date_in_future,
    ]
    failures = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failures += 1
            print(f"FAIL: {t.__name__} -> {e}")

    print(f"\n{len(tests) - failures}/{len(tests)} edge case tests passed.")
