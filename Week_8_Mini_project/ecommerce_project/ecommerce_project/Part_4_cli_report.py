"""
Part 4: Python + SQL Integration
----------------------------------
A command-line tool that:
    1. Takes user input for report type (daily/weekly/monthly)
    2. Takes a date range as input
    3. Connects to the SQLite database
    4. Generates a summary report showing:
        - Total orders, revenue, unique customers
        - Top 3 products
        - Comparison with the previous period (% change)

Uses only sqlite3 (no external libraries), as required.

Usage (interactive):
    python3 cli_report.py

Usage (non-interactive, for scripting/testing):
    python3 cli_report.py --type monthly --start 2024-01-01 --end 2024-01-31
"""

import sqlite3
import sys
import argparse
from datetime import datetime, timedelta

DB_PATH = "db/ecommerce.db"


def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d")


def get_period_stats(conn, start_date, end_date):
    """Returns (total_orders, total_revenue, unique_customers, top_3_products)."""
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(DISTINCT o.order_id), COUNT(DISTINCT o.customer_id)
        FROM orders o
        WHERE date(o.order_date) BETWEEN ? AND ?
    """, (start_date, end_date))
    total_orders, unique_customers = cur.fetchone()
    total_orders = total_orders or 0
    unique_customers = unique_customers or 0

    cur.execute("""
        SELECT COALESCE(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 0)
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE date(o.order_date) BETWEEN ? AND ?
    """, (start_date, end_date))
    total_revenue = cur.fetchone()[0] or 0

    cur.execute("""
        SELECT p.product_name,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE date(o.order_date) BETWEEN ? AND ?
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT 3
    """, (start_date, end_date))
    top_products = cur.fetchall()

    return total_orders, total_revenue, unique_customers, top_products


def previous_period(start_date, end_date):
    """Given a period, returns the immediately preceding period of the same length."""
    start = parse_date(start_date)
    end = parse_date(end_date)
    length = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=length - 1)
    return prev_start.strftime("%Y-%m-%d"), prev_end.strftime("%Y-%m-%d")


def pct_change(old, new):
    if old == 0:
        return None
    return round((new - old) / old * 100, 2)


def generate_report(report_type, start_date, end_date):
    conn = sqlite3.connect(DB_PATH)

    orders, revenue, customers, top_products = get_period_stats(conn, start_date, end_date)
    prev_start, prev_end = previous_period(start_date, end_date)
    p_orders, p_revenue, p_customers, _ = get_period_stats(conn, prev_start, prev_end)

    conn.close()

    print("\n" + "=" * 60)
    print(f"  {report_type.upper()} REPORT: {start_date} to {end_date}")
    print("=" * 60)
    print(f"  Total Orders:      {orders}")
    print(f"  Total Revenue:     {revenue:,.2f}")
    print(f"  Unique Customers:  {customers}")
    print("\n  Top 3 Products:")
    if top_products:
        for i, (name, rev) in enumerate(top_products, start=1):
            print(f"    {i}. {name:<35} {rev:,.2f}")
    else:
        print("    (no sales in this period)")

    print(f"\n  Comparison with previous period ({prev_start} to {prev_end}):")
    orders_chg = pct_change(p_orders, orders)
    revenue_chg = pct_change(p_revenue, revenue)
    customers_chg = pct_change(p_customers, customers)
    print(f"    Orders:     {p_orders} -> {orders}   ({fmt_pct(orders_chg)})")
    print(f"    Revenue:    {p_revenue:,.2f} -> {revenue:,.2f}   ({fmt_pct(revenue_chg)})")
    print(f"    Customers:  {p_customers} -> {customers}   ({fmt_pct(customers_chg)})")
    print("=" * 60 + "\n")


def fmt_pct(value):
    if value is None:
        return "N/A (no prior data)"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value}%"


def interactive_mode():
    print("=== E-Commerce Report Generator ===")
    report_type = ""
    while report_type not in ("daily", "weekly", "monthly"):
        report_type = input("Report type (daily/weekly/monthly): ").strip().lower()

    start_date = input("Start date (YYYY-MM-DD): ").strip()
    end_date = input("End date (YYYY-MM-DD): ").strip()

    try:
        parse_date(start_date)
        parse_date(end_date)
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)

    generate_report(report_type, start_date, end_date)


def main():
    parser = argparse.ArgumentParser(description="Generate an e-commerce order summary report.")
    parser.add_argument("--type", choices=["daily", "weekly", "monthly"], help="Report type")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    if args.type and args.start and args.end:
        generate_report(args.type, args.start, args.end)
    else:
        interactive_mode()


if __name__ == "__main__":
    main()
