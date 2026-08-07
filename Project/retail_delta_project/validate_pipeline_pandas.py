"""
Local validation harness for retail_delta_project.

Mirrors the logic in notebooks/01-04 using pandas instead of PySpark, so it
can run without a Spark cluster. This is NOT a replacement for running the
real notebooks in Databricks (that's still required for the assignment) —
it's a fast way to catch logic bugs before you spend cluster time.
"""
import glob
import hashlib
import os
import re

import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
BATCH = os.path.join(BASE, "datasets", "batch")
INCR = os.path.join(BASE, "datasets", "incremental")

FAR_FUTURE = pd.Timestamp("9999-12-31")
INITIAL_LOAD_DATE = pd.Timestamp("2020-01-01")

pd.set_option("display.width", 120)


def sha(*parts):
    return hashlib.sha256("||".join(str(p) for p in parts).encode()).hexdigest()


# ----------------------------------------------------------------- BRONZE
def load_bronze():
    customers = pd.read_csv(f"{BATCH}/customers_batch.csv", dtype=str)
    products = pd.read_csv(f"{BATCH}/products_batch.csv", dtype=str)
    stores = pd.read_csv(f"{BATCH}/stores_batch.csv", dtype=str)
    orders = pd.read_csv(f"{BATCH}/orders_batch.csv", dtype=str)

    incr_orders, cust_cdc, prod_cdc = [], [], []
    for day_dir in sorted(glob.glob(f"{INCR}/day_*")):
        for f in glob.glob(f"{day_dir}/orders_incremental_*.csv"):
            incr_orders.append(pd.read_csv(f, dtype=str))
        for f in glob.glob(f"{day_dir}/customers_cdc_*.csv"):
            cust_cdc.append(pd.read_csv(f, dtype=str))
        for f in glob.glob(f"{day_dir}/products_cdc_*.csv"):
            prod_cdc.append(pd.read_csv(f, dtype=str))

    orders_incr = pd.concat(incr_orders, ignore_index=True)
    customers_cdc = pd.concat(cust_cdc, ignore_index=True)
    products_cdc = pd.concat(prod_cdc, ignore_index=True)

    print(f"[bronze] orders_batch={len(orders)} orders_incremental={len(orders_incr)} "
          f"customers={len(customers)} products={len(products)} stores={len(stores)} "
          f"customers_cdc={len(customers_cdc)} products_cdc={len(products_cdc)}")
    print(f"[bronze] schema check -> coupon_code present on "
          f"{orders_incr['coupon_code'].notna().sum() if 'coupon_code' in orders_incr.columns else 0} rows "
          f"(should only be day 2026-04-26)")

    return orders, orders_incr, customers, products, stores, customers_cdc, products_cdc


# ------------------------------------------------------------ SILVER STG1
def extract_price(series):
    return pd.to_numeric(series.astype(str).str.extract(r"(-?\d+\.?\d*)")[0], errors="coerce")


def clean_orders(orders_batch, orders_incr):
    o = pd.concat([orders_batch, orders_incr], ignore_index=True)
    o["order_date_cast"] = pd.to_datetime(o["order_date"], errors="coerce", format="%Y-%m-%d")
    o["unit_price_cast"] = extract_price(o["unit_price"])
    o["quantity_cast"] = pd.to_numeric(o["quantity"], errors="coerce")

    valid = (
        o["order_id"].notna() & (o["order_id"] != "") &
        o["customer_id"].notna() & (o["customer_id"] != "") &
        o["product_id"].notna() & (o["product_id"] != "") &
        o["order_date_cast"].notna() &
        o["unit_price_cast"].notna() & (o["unit_price_cast"] > 0) &
        o["quantity_cast"].notna() & (o["quantity_cast"] > 0)
    )
    good, quarantine = o[valid].copy(), o[~valid].copy()

    # dedup: keep last occurrence per order_id (proxy for "latest ingestion wins")
    good = good.drop_duplicates(subset="order_id", keep="last")

    # drop the raw string columns before renaming the cast ones over them,
    # otherwise pandas ends up with two "order_date" columns
    good = good.drop(columns=["order_date", "unit_price", "quantity"])
    clean = good.rename(columns={"order_date_cast": "order_date", "unit_price_cast": "unit_price",
                                  "quantity_cast": "quantity"})
    keep_cols = ["order_id", "customer_id", "product_id", "store_id", "order_date",
                 "quantity", "unit_price"] + (["coupon_code"] if "coupon_code" in clean.columns else [])
    clean = clean[keep_cols]
    print(f"[silver1] orders_clean={len(clean)} orders_quarantine={len(quarantine)}")
    return clean, quarantine


def clean_customers(customers):
    c = customers.copy()
    c["signup_date"] = pd.to_datetime(c["signup_date"], errors="coerce", format="%Y-%m-%d")
    c["city"] = c["city"].fillna("Unknown").replace("", "Unknown")
    c["segment"] = c["segment"].fillna("Unknown").replace("", "Unknown")
    valid = c["customer_id"].notna() & (c["customer_id"] != "")
    good, quarantine = c[valid].copy(), c[~valid].copy()
    good = good.drop_duplicates(subset="customer_id", keep="last")
    print(f"[silver1] customers_clean={len(good)} customers_quarantine={len(quarantine)}")
    return good, quarantine


def clean_products(products):
    p = products.copy()
    p["unit_price"] = extract_price(p["unit_price"])
    p["category"] = p["category"].fillna("Unknown").replace("", "Unknown")
    valid = p["product_id"].notna() & (p["product_id"] != "") & p["unit_price"].notna() & (p["unit_price"] > 0)
    good, quarantine = p[valid].copy(), p[~valid].copy()
    good = good.drop_duplicates(subset="product_id", keep="last")
    print(f"[silver1] products_clean={len(good)} products_quarantine={len(quarantine)}")
    return good, quarantine


def clean_stores(stores):
    return stores.drop_duplicates(subset="store_id", keep="last")


# ------------------------------------------------------------ SILVER STG2
def seed_scd2(clean_df, business_key, attr_cols, start_date_col):
    df = clean_df.copy()
    df["hash_value"] = df.apply(lambda r: sha(*[r[c] for c in attr_cols]), axis=1)
    df["effective_start_date"] = df[start_date_col].fillna(INITIAL_LOAD_DATE)
    df["effective_end_date"] = FAR_FUTURE
    df["is_current"] = True
    df[f"{business_key}_sk"] = df.apply(
        lambda r: sha(r[business_key], r["effective_start_date"]), axis=1)
    return df[[f"{business_key}_sk", business_key, *attr_cols,
               "effective_start_date", "effective_end_date", "is_current", "hash_value"]]


def apply_cdc_day(dim, cdc_day, business_key, attr_cols, change_date):
    cdc_day = cdc_day.drop_duplicates(subset=business_key, keep="last").copy()
    cdc_day["hash_value"] = cdc_day.apply(lambda r: sha(*[r[c] for c in attr_cols]), axis=1)
    cdc_day["change_date"] = change_date

    active = dim[dim["is_current"]]
    merged = cdc_day.merge(active[[business_key, "hash_value"]], on=business_key,
                            how="left", suffixes=("", "_active"))
    changed_keys = merged.loc[merged["hash_value"] != merged["hash_value_active"], business_key]

    # Step 1: expire changed active rows
    expire_mask = dim["is_current"] & dim[business_key].isin(changed_keys)
    dim.loc[expire_mask, "is_current"] = False
    dim.loc[expire_mask, "effective_end_date"] = change_date - pd.Timedelta(days=1)

    # Step 2: insert new open rows for changed + brand-new keys
    current_active = dim[dim["is_current"]][[business_key, "hash_value"]]
    to_insert = cdc_day.merge(current_active, on=[business_key, "hash_value"], how="left",
                               indicator=True)
    to_insert = to_insert[to_insert["_merge"] == "left_only"].drop(columns="_merge")
    to_insert["effective_start_date"] = to_insert["change_date"]
    to_insert["effective_end_date"] = FAR_FUTURE
    to_insert["is_current"] = True
    to_insert[f"{business_key}_sk"] = to_insert.apply(
        lambda r: sha(r[business_key], r["effective_start_date"]), axis=1)
    to_insert = to_insert[[f"{business_key}_sk", business_key, *attr_cols,
                            "effective_start_date", "effective_end_date", "is_current", "hash_value"]]

    return pd.concat([dim, to_insert], ignore_index=True), len(to_insert)


def build_scd2(clean_df, cdc_df, business_key, attr_cols, start_date_col=None):
    if start_date_col:
        dim = seed_scd2(clean_df, business_key, attr_cols, start_date_col)
    else:
        seed_df = clean_df.copy()
        seed_df["_seed_date"] = INITIAL_LOAD_DATE
        dim = seed_scd2(seed_df, business_key, attr_cols, "_seed_date")

    cdc_df = cdc_df.copy()
    cdc_df["change_date"] = pd.to_datetime(cdc_df["change_date"], errors="coerce")
    for day, day_df in cdc_df.groupby("change_date"):
        dim, n = apply_cdc_day(dim, day_df, business_key, attr_cols, day)
        print(f"  CDC {day.date()}: {n} new/changed dimension rows")
    return dim


# --------------------------------------------------------------------GOLD
def build_fact(orders_clean, dim_customer, dim_product, dim_store):
    o = orders_clean.copy()
    o["order_date"] = pd.to_datetime(o["order_date"])

    def resolve(df, dim, business_key, sk_col):
        merged = df.merge(dim, on=business_key, how="left", suffixes=("", "_dim"))
        in_range = (merged["order_date"] >= merged["effective_start_date"]) & \
                   (merged["order_date"] <= merged["effective_end_date"])
        merged = merged[in_range | merged["effective_start_date"].isna()]
        return merged

    step1 = resolve(o, dim_customer.rename(columns={"customer_id_sk": "customer_sk"}),
                     "customer_id", "customer_sk")
    step1 = step1.drop_duplicates(subset="order_id", keep="first")

    keep_cust_cols = ["order_id", "customer_id", "product_id", "store_id", "order_date",
                       "quantity", "unit_price", "customer_sk", "city", "segment"]
    keep_cust_cols = [c for c in keep_cust_cols if c in step1.columns]
    step1 = step1[keep_cust_cols].rename(columns={"segment": "customer_segment"})

    step2 = step1.merge(
        dim_product.rename(columns={"product_id_sk": "product_sk"}),
        on="product_id", how="left", suffixes=("", "_p"))
    in_range = (step2["order_date"] >= step2["effective_start_date"]) & \
               (step2["order_date"] <= step2["effective_end_date"])
    step2 = step2[in_range | step2["effective_start_date"].isna()]
    step2 = step2.drop_duplicates(subset="order_id", keep="first")

    fact = step2.merge(dim_store.rename(columns={"store_id": "store_id_dim"}),
                        left_on="store_id", right_on="store_id_dim", how="left")
    fact["revenue"] = fact["quantity"] * fact["unit_price"]
    fact = fact.rename(columns={"category": "product_category", "region": "store_region"})

    # Fallback: orders that predate their customer/product's earliest dimension
    # version get resolved against the earliest available version instead of
    # being left null (mirrors the same fallback added to 04_gold notebook).
    cust_first = (dim_customer.sort_values("effective_start_date")
                  .drop_duplicates(subset="customer_id", keep="first")
                  [["customer_id", "customer_id_sk", "segment"]]
                  .rename(columns={"customer_id_sk": "customer_sk_fb", "segment": "segment_fb"}))
    prod_first = (dim_product.sort_values("effective_start_date")
                  .drop_duplicates(subset="product_id", keep="first")
                  [["product_id", "product_id_sk", "category"]]
                  .rename(columns={"product_id_sk": "product_sk_fb", "category": "category_fb"}))

    fact = fact.merge(cust_first, on="customer_id", how="left")
    fact = fact.merge(prod_first, on="product_id", how="left")
    fact["customer_key_is_estimated"] = fact["customer_sk"].isna() & fact["customer_sk_fb"].notna()
    fact["product_key_is_estimated"] = fact["product_sk"].isna() & fact["product_sk_fb"].notna()
    fact["customer_sk"] = fact["customer_sk"].fillna(fact["customer_sk_fb"])
    fact["product_sk"] = fact["product_sk"].fillna(fact["product_sk_fb"])
    fact["customer_segment"] = fact["customer_segment"].fillna(fact["segment_fb"])
    fact["product_category"] = fact["product_category"].fillna(fact["category_fb"])

    cols = ["order_id", "customer_id", "product_id", "customer_sk", "product_sk", "store_id",
            "order_date", "quantity", "unit_price", "revenue", "customer_segment",
            "product_category", "store_region", "customer_key_is_estimated", "product_key_is_estimated"]
    fact = fact[[c for c in cols if c in fact.columns]]

    resolved_mask = fact["customer_sk"].notna() & fact["product_sk"].notna()
    fact_resolved = fact[resolved_mask].drop(columns=["customer_id", "product_id"])
    fact_quarantine = fact[~resolved_mask]
    estimated = (fact_resolved["customer_key_is_estimated"] | fact_resolved["product_key_is_estimated"]).sum()

    print(f"[gold] fact_orders={len(fact_resolved)} resolved_via_fallback={estimated} "
          f"referential_integrity_quarantine={len(fact_quarantine)}")
    if len(fact_quarantine):
        orphan_products = fact_quarantine.loc[fact_quarantine["product_sk"].isna(), "product_id"].unique()
        orphan_customers = fact_quarantine.loc[fact_quarantine["customer_sk"].isna(), "customer_id"].unique()
        print(f"  orphaned product_ids (no dim row at all, {len(orphan_products)} total): "
              f"{sorted(orphan_products)[:10]}")
        print(f"  orphaned customer_ids ({len(orphan_customers)} total): {sorted(orphan_customers)[:10]}")
    return fact_resolved, fact_quarantine


def build_gold_aggregates(fact):
    daily = (fact.groupby("order_date")
             .agg(total_orders=("order_id", "nunique"),
                  total_revenue=("revenue", "sum"),
                  total_units=("quantity", "sum"))
             .reset_index())
    daily["avg_order_value"] = (daily["total_revenue"] / daily["total_orders"]).round(2)

    category = (fact.groupby("product_category")
                .agg(total_orders=("order_id", "nunique"),
                     total_revenue=("revenue", "sum"),
                     total_units=("quantity", "sum"))
                .sort_values("total_revenue", ascending=False).reset_index())

    segment = (fact.groupby("customer_segment")
               .agg(unique_customers=("customer_sk", "nunique"),
                    total_orders=("order_id", "nunique"),
                    total_revenue=("revenue", "sum"))
               .sort_values("total_revenue", ascending=False).reset_index())

    region = (fact.groupby("store_region")
              .agg(total_orders=("order_id", "nunique"),
                   total_revenue=("revenue", "sum"),
                   total_units=("quantity", "sum"))
              .sort_values("total_revenue", ascending=False).reset_index())

    return daily, category, segment, region


def main():
    (orders_batch, orders_incr, customers, products, stores,
     customers_cdc, products_cdc) = load_bronze()

    orders_clean, orders_q = clean_orders(orders_batch, orders_incr)
    customers_clean, customers_q = clean_customers(customers)
    products_clean, products_q = clean_products(products)
    dim_store = clean_stores(stores)

    print("\n[silver2] building dim_customer_scd2 ...")
    dim_customer = build_scd2(customers_clean, customers_cdc, "customer_id",
                               ["customer_name", "email", "city", "segment"],
                               start_date_col="signup_date")
    multi_version_customers = dim_customer["customer_id"].value_counts()
    print(f"  total dim_customer_scd2 rows: {len(dim_customer)} "
          f"(customers with >1 version: {(multi_version_customers > 1).sum()})")

    print("\n[silver2] building dim_product_scd2 ...")
    dim_product = build_scd2(products_clean, products_cdc, "product_id",
                              ["product_name", "category", "unit_price"])
    multi_version_products = dim_product["product_id"].value_counts()
    print(f"  total dim_product_scd2 rows: {len(dim_product)} "
          f"(products with >1 version: {(multi_version_products > 1).sum()})")

    print("\n[gold] building fact_orders + aggregates ...")
    fact, fact_quarantine = build_fact(orders_clean, dim_customer, dim_product, dim_store)
    daily, category, segment, region = build_gold_aggregates(fact)

    print("\n=== gold_daily_sales (first 5) ===")
    print(daily.head())
    print("\n=== gold_category_sales ===")
    print(category)
    print("\n=== gold_segment_sales ===")
    print(segment)
    print("\n=== gold_region_sales ===")
    print(region)


if __name__ == "__main__":
    main()
