# Retail E-commerce Sales Analytics Pipeline — Starter Kit

This is a working implementation of the CEI intern assignment: Databricks +
Delta Lake, medallion architecture, SCD Type 2 dimensions.

## What's in here

```
retail_delta_project/
├── generate_datasets.py         # already run — produced datasets/ below
├── datasets/
│   ├── batch/                   # orders/customers/products/stores (dirty, 12k+ orders)
│   └── incremental/day_YYYY-MM-DD/   # 5 days of CDC + daily orders, schema change on day 4
└── notebooks/
    ├── 00_config.py             # catalog/schema/volume + shared path vars
    ├── 01_bronze_ingestion.py   # raw landing, audit cols, Auto Loader for incremental
    ├── 02_silver1_cleaning.py   # cast/clean/dedup/quarantine
    ├── 03_silver2_scd2.py       # SCD Type 2 customer + product dimensions
    ├── 04_gold_fact_and_analytics.py   # point-in-time fact join + 4 gold tables
    └── 05_delta_features_demo.py       # history / time travel / schema evolution
```

## How to run it

1. **Upload the data.** In Databricks: Catalog Explorer → create/open the
   `retail_demo.raw.retail_files` volume → upload the whole `datasets/` folder
   so it lands at
   `/Volumes/retail_demo/raw/retail_files/retail_delta_project/datasets/...`
   (or use the Databricks CLI: `databricks fs cp -r ./datasets dbfs:/Volumes/retail_demo/raw/retail_files/retail_delta_project/datasets`).
   If you don't have Unity Catalog, swap `BASE_PATH` in `00_config.py` for a
   DBFS path like `/dbfs/FileStore/retail_delta_project` and use
   `CREATE DATABASE` instead of `CREATE CATALOG`/`CREATE VOLUME`.

2. **Import the notebooks** into your workspace (Workspace → Import → select
   all 6 `.py` files; Databricks recognizes the `# Databricks notebook source`
   header and turns them into proper notebooks with cells).

3. **Run in order**: `00 → 01 → 02 → 03 → 04 → 05`. `00_config` is also
   `%run` from inside 01–04, so you don't have to run it manually every time,
   but running it once first (to create the catalog/schema/volume) is a good
   sanity check.

4. **Expect one deliberate failure** in `01_bronze_ingestion`: the Auto Loader
   stream over the incremental orders will throw a schema-change error the
   first time it hits `orders_incremental_2026-04-26.csv` (new `coupon_code`
   column). Re-run that cell — Auto Loader updates its schema log and
   continues. This is the exact "Detected schema change" scenario from the
   guide's error table, and demonstrating you understand *why* it happens
   (rather than just re-running blindly) is worth mentioning in your
   write-up.

## Known data-quality finding (worth a line in your write-up)

Running the pipeline surfaces **13 product_ids that are referenced by ~909
orders but have zero valid rows in `dim_product_scd2`** — their `unit_price`
was corrupted (`"unknown"` etc.) in the original batch file, so Silver Stage 1
correctly quarantined them, and no later CDC update ever supplied a valid
price to bring them back. Gold Stage 4 now:
- resolves the point-in-time join as usual,
- **falls back to the earliest available dimension version** for the (much
  rarer) case of an order genuinely predating its customer's first known
  record,
- and routes the 909 truly orphaned rows to
  `gold.fact_orders_referential_quarantine` instead of leaving null surrogate
  keys in `fact_orders` or silently dropping them.

This is a legitimate referential-integrity check to call out explicitly in
your submission — it demonstrates you're validating fact-to-dimension
integrity, not just running the joins and hoping for the best.

## Running locally in VS Code (no Databricks needed)

`run_local_pipeline.py` is a Databricks-free version of the whole pipeline
(Bronze → Silver 1 → SCD2 → Gold) using open-source PySpark + Delta Lake. It
writes real Delta tables to `./local_warehouse` on your machine.

**I could not execute this script myself** (no Java/network in the sandbox
that built it) — the logic mirrors the validated Databricks notebooks and
`validate_pipeline_pandas.py` closely, but treat the first run as a real test
run, not a guaranteed-clean one. Paste me any error and I'll fix it against
the real thing.

Setup:

```bash
# 1. Java 11 or 17 must be on PATH — check with:
java -version

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install PySpark + Delta Lake (versions must be compatible)
pip install pyspark==3.5.1 delta-spark==3.1.0

# 4. Run it
python run_local_pipeline.py
```

What you'll see: progress prints for every Bronze/Silver/Gold table as it's
built, ending with the category and region sales tables printed to the
terminal. Everything lands as real Delta tables under `./local_warehouse` —
open a Python REPL or another script and `spark.table("retail_demo_gold.gold_daily_sales").show()`
any time afterward (same SparkSession setup as the top of the script).

**What's different from the Databricks notebooks:** no `dbutils`, no Unity
Catalog/Volumes, and incremental files are read with a plain batch glob read
instead of Auto Loader (`cloudFiles` doesn't exist outside Databricks) — see
the docstring at the top of the file for details. The SCD2 MERGE logic,
point-in-time Gold join, and referential-integrity quarantine are unchanged.

If VS Code's Python extension flags import errors for `pyspark`/`delta`
before you've activated `.venv`, make sure VS Code's selected interpreter
(bottom-right corner, or **Python: Select Interpreter** in the command
palette) points at `.venv`, not your system Python.

## Running locally without Spark (sanity-check only)

`validate_pipeline_pandas.py` re-implements the same Bronze→Silver→SCD2→Gold
logic in pandas so you can sanity-check the approach without a cluster:

```bash
python3 validate_pipeline_pandas.py
```

This is **not** a substitute for running the real notebooks in Databricks —
it doesn't touch Delta Lake, Auto Loader, or MERGE at all — but it's a fast
way to catch logic bugs (wrong join keys, off-by-one date ranges, etc.)
before spending cluster time.

## Design decisions worth knowing (in case you're asked about them)

- **Surrogate keys** = `sha2(business_key || effective_start_date)`. This
  matches the updated guide's hint (hash of business key + start date) rather
  than `monotonically_increasing_id()` — it's deterministic, so re-running a
  day's SCD2 merge idempotently produces the same key instead of a new random
  one.
- **SCD2 merge** is the two-step pattern from the guide: Step 1 expires any
  currently-active row whose hash changed; Step 2 anti-joins the incoming CDC
  rows against the (now-updated) current rows on `(business_key, hash_value)`
  to find exactly the rows that need a fresh open version — this correctly
  covers both brand-new business keys and changed ones in one pass.
- **Late-arriving orders**: the dataset generator injects some orders whose
  `order_date` is earlier than the incremental file's calendar day (up to 10
  days late). Nothing special is required in the pipeline — as the guide
  notes, the point-in-time join in Gold resolves them correctly as long as
  the relevant dimension version existed by that order_date.
- **Quarantine tables** (`silver1_orders_quarantine`,
  `silver1_customers_quarantine`, `silver1_products_quarantine`) hold
  everything that failed a validity check, so you can inspect *why* rows were
  dropped instead of just silently losing them.
- **Point-in-time fact join**: `order_date BETWEEN effective_start_date AND
  effective_end_date` on both customer and product dimensions — not a plain
  equi-join on the business key — so the fact table reflects the customer's
  city/segment and the product's category/price *as they were on the day of
  the order*.
- **Community Edition support**: `00_config.py` has a `USE_UNITY_CATALOG`
  flag. With it off, everything uses plain 2-level `schema.table` names
  against the `hive_metastore` instead of 3-level `catalog.schema.table`
  names, since Community Edition doesn't support Unity Catalog.

## If you want to regenerate the datasets

```bash
python3 generate_datasets.py
```

Re-running is deterministic (seeded RNG) unless you change `random.seed(42)`
or the row-count constants at the top of the script.
