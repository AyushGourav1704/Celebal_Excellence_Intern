# 🛒 E-Commerce Order Analytics System

> A complete **Python + SQLite** based data analytics pipeline that generates synthetic e-commerce data, cleans and validates it, stores it in SQLite, executes SQL analytics, and generates business reports through a Command Line Interface (CLI).

**Internship Project:** Celebal Technologies – Week 8 Mini Project

---

# 📌 Project Overview

This project simulates a real-world e-commerce analytics workflow by building an end-to-end data pipeline.

The pipeline performs:

- 📦 Raw data generation
- 🧹 Data cleaning & validation
- 🗄️ SQLite database loading
- 📊 SQL business analytics
- 📈 CLI reporting
- ✅ Edge case testing

---

# 📂 Project Structure

```text
Week_8_Mini_project/
│
├── ecommerce_project/
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── order_items.csv
│
├── cleaned_data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── order_items.csv
│
├── db/
│   └── ecommerce.db
│
├── reports/
│   ├── data_quality_report.txt
│   └── query_outputs.txt
│
├── ss_project/
│   ├── part_1.png
│   ├── part_2.png
│   ├── load_db.png
│   ├── run_queries.png
│   ├── part_4.png
│   └── part_5.png
│
├── Part_1_generate_data.py
├── Part_2_Data_cleaning.py
├── load_db.py
├── part_3_queries.sql
├── run_queries.py
├── Part_4_cli_report.py
├── Part_5_test_edge_cases.py
└── README.md
```

---

# 🚀 Features

- Generate realistic e-commerce datasets
- Data cleaning using Pandas
- Missing value handling
- Duplicate removal
- Email validation
- Referential integrity checks
- SQLite database creation
- SQL analytics queries
- CLI reporting tool
- Automated edge case testing

---

# 🛠️ Tech Stack

| Technology | Purpose |
|------------|----------|
| Python | Core Programming |
| SQLite | Database |
| Pandas | Data Cleaning |
| SQL | Analytics |
| CSV | Dataset Storage |
| Argparse | CLI Tool |

---

# 🔄 Project Workflow

```text
Generate Raw CSV Files
          │
          ▼
Clean & Validate Data
          │
          ▼
Load into SQLite Database
          │
          ▼
Execute SQL Queries
          │
          ▼
Generate Reports
          │
          ▼
Run Edge Case Tests
```

---

# ▶️ How to Run

## 1️⃣ Generate Dataset

```bash
python Part_1_generate_data.py
```

---

## 2️⃣ Clean Dataset

```bash
python Part_2_Data_cleaning.py
```

---

## 3️⃣ Load SQLite Database

```bash
python load_db.py
```

---

## 4️⃣ Execute SQL Queries

```bash
python run_queries.py
```

---

## 5️⃣ Generate CLI Report

Interactive Mode

```bash
python Part_4_cli_report.py
```

Monthly Report

```bash
python Part_4_cli_report.py --type monthly --start 2024-06-01 --end 2024-06-30
```

---

## 6️⃣ Run Edge Case Tests

```bash
python Part_5_test_edge_cases.py
```

---

# 📊 SQL Analytics

The project performs various business analyses including:

- Total Revenue
- Monthly Revenue
- Top Customers
- Top Products
- Order Status Analysis
- Average Order Value
- Discount Analysis
- Return Analysis
- Category-wise Revenue
- Customer Purchase Trends
- Revenue Trends
- Ranking Functions
- Window Functions
- Business Insights

---

# 📄 Reports Generated

### Data Quality Report

```
reports/data_quality_report.txt
```

Contains

- Missing Values
- Duplicate Records
- Invalid Emails
- Invalid Prices
- Referential Integrity Issues

---

### Query Output Report

```
reports/query_outputs.txt
```

Contains outputs of all SQL analytical queries.

---

# 💰 Revenue Formula

```text
Revenue = Quantity × Unit Price × (1 − Discount / 100)
```

---

# ✅ Edge Cases Tested

- Invalid Email IDs
- Missing Values
- Duplicate Records
- Invalid Order IDs
- Discount > 100%
- Negative Quantity (Returns)
- Referential Integrity Validation

# 🎯 Learning Outcomes

This project helped in understanding:

- Data Generation
- Data Cleaning
- Data Validation
- SQLite Database Management
- SQL Analytics
- Python Automation
- CLI Development
- Business Reporting
- Data Quality Management

---

# 👨‍💻 Author

**Ayush Gourav**

B.Tech – Computer Science & Engineering

**Celebal Technologies Internship**

Week 8 Mini Project