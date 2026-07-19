# Week 5 - Spark Data Cleaning using PySpark

## Project Overview

This project demonstrates the fundamentals of **Apache Spark** and **PySpark DataFrames** by performing data cleaning, transformation, filtering, aggregation, and analysis on a sample transaction dataset.

The assignment covers the complete data cleaning workflow and answers all 15 PySpark assignment questions using practical examples.

---

## Objectives

- Understand Apache Spark fundamentals
- Work with PySpark DataFrames
- Perform data cleaning operations
- Apply filtering and aggregation
- Handle missing and duplicate data
- Process inconsistent timestamp formats
- Build a simple data processing pipeline

---

## Technologies Used

- Python 3.x
- Apache Spark (PySpark)
- Jupyter Notebook
- Anaconda
- Pandas (for dataset generation)

---

## Dataset

Since the assignment did not provide a dataset, a synthetic transaction dataset was created with realistic business data.

### Dataset Features

- 620 transaction records
- Duplicate records
- Missing values
- Mixed timestamp formats
- Customer demographics
- Product categories
- Store information
- Subscription details

### Columns

- user_id
- username
- email
- age
- gender
- city
- region
- product_category
- subscription
- store_id
- price
- quantity
- status
- transaction_date
- raw_timestamp

---

## Assignment Tasks Completed

### Data Loading
- Loaded CSV into Spark DataFrame
- Used `inferSchema=True`
- Displayed schema and preview

### Data Cleaning
- Removed duplicate records
- Handled missing values
- Filled null prices
- Filled missing status
- Cleaned username and email fields

### Data Transformation
- Applied filtering conditions
- Selected required columns
- Created transformed DataFrames

### Aggregation
Performed analysis using:

- count()
- sum()
- avg()
- min()
- max()
- groupBy()

### Timestamp Handling

Handled multiple timestamp formats using:

- `try_to_timestamp()`
- `coalesce()`

instead of relying only on automatic schema inference.

### Final Pipeline

Implemented a complete Spark pipeline:

```
Read CSV
      ↓
Remove Duplicates
      ↓
Handle Null Values
      ↓
Filter Data
      ↓
Group By Store
      ↓
Calculate Total Revenue
      ↓
Export Results
```

---

## Assignment Questions Covered

This notebook contains solutions for all **15 assignment questions (Q1–Q15)** including:

- Theory explanations
- PySpark code
- Output screenshots
- Final observations

---

## Key Observations

- Found **20 duplicate records**, which were removed using `dropDuplicates()`.
- Mixed timestamp formats required multiple parsing attempts with `try_to_timestamp()` and `coalesce()`.
- Mumbai, Delhi, and Bangalore contained the highest number of transactions.
- Premium users between **18–30 years** formed a valuable customer segment.
- Generated store-wise revenue after cleaning the dataset.

---

## Project Structure

```
Week_5_Spark_data_cleaning/
│
├── data/
│   └── dataset.csv
│
├── notebook/
│   ├── spark_basics.ipynb
│   └── .ipynb_checkpoints/
│
├── output/
│   └── results.csv
│
├── 02_q3_duplicates.png
├── 03_q4_groupby_region.png
├── 04_q6_city_count.png
├── 05_q8_filter.png
├── 06_q10_timestamp.png
├── 07_q12_email_username.png
├── 08_q13_price_stats.png
├── 09_q15_final_pipeline.png
├── 10_insights.png
├── 11_saved_results.png
│
└── README.md
```

---

## How to Run

### 1. Clone the Repository

```bash
git clone <repository-url>
```

### 2. Navigate to the Project

```bash
cd Week_5_Spark_data_cleaning
```

### 3. Open the Notebook

Launch Jupyter Notebook using Anaconda or VS Code.

```bash
jupyter notebook
```

Open:

```
notebook/spark_basics.ipynb
```

### 4. Run All Cells

Execute every notebook cell from top to bottom.

The notebook will:

- Create Spark Session
- Load the dataset
- Perform cleaning
- Execute all assignment tasks
- Save the final output

---

## Output

The processed data is exported to:

```
output/results.csv
```

The repository also includes screenshots of important outputs for each assignment question.

---

## Learning Outcomes

After completing this assignment, I gained practical experience with:

- Apache Spark
- PySpark DataFrames
- Data Cleaning
- Handling Missing Values
- Removing Duplicates
- Filtering Data
- GroupBy Operations
- Aggregation Functions
- Timestamp Parsing
- Data Processing Pipelines

---

## Author

**Ayush Gourav**

B.Tech (Computer Science & Engineering)

Celebal Technologies Summer Internship 2026

Week 5 Assignment – Spark Data Cleaning