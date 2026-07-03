# Week 3 - SQL Advanced Analytics

## Objective

Analyze the Superstore dataset using advanced SQL concepts such as
Subqueries, Common Table Expressions (CTEs), Window Functions, and JOINs
to generate meaningful business insights.

## Dataset

-   Sample Superstore Dataset (CSV)

## Files

-   SuperStore_raw.sql
-   README.md

## Assignment Tasks

### Step 1: Data Setup

-   Imported the Superstore dataset into the `superstore_raw` staging
    table.
-   Created separate `customers`, `orders`, and `products` tables.
-   Used `SELECT DISTINCT` to populate the tables with unique records.

### Step 2: SQL Queries

#### Subqueries

-   Find all orders where sales are greater than the average sales.
-   Find the highest sales order for each customer.

#### Common Table Expressions (CTEs)

-   Calculate total sales for each customer.
-   Identify customers whose total sales are above average.

#### Window Functions

-   Assign `ROW_NUMBER()` to each order within a customer.
-   Rank customers based on total sales using `RANK()`.
-   Display the Top 3 customers based on total sales.

### Step 3: Final Combined Query

Combined **JOIN**, **CTE**, and **Window Functions** to display: -
Customer Name - Total Sales - Customer Rank

## Mini Project -- Customer Sales Insights

1.  Who are the Top 5 customers?
2.  Who are the Bottom 5 customers?
3.  Which customers made only one order?
4.  Which customers have above-average sales?
5.  What is the highest order value per customer?

## SQL Concepts Used

-   SELECT DISTINCT
-   Aggregate Functions (`SUM`, `AVG`, `MAX`)
-   GROUP BY
-   HAVING
-   Subqueries
-   Common Table Expressions (CTEs)
-   INNER JOIN
-   ROW_NUMBER()
-   RANK()
-   DENSE_RANK()
-   PARTITION BY
-   ORDER BY
-   LIMIT

## Business Insights

-   Identified customers with above-average sales.
-   Ranked customers based on total sales.
-   Found the highest-value order for each customer.
-   Identified top-performing and low-performing customers.
-   Detected customers who placed only one order.
-   Combined JOINs, CTEs, and Window Functions to generate customer
    rankings and sales insights.

## Tools Used

-   PostgreSQL
-   DBeaver
-   Git
-   GitHub

## Author

**Ayush Gourav**

-   B.Tech CSE (2023--2027)
-   ITER, Siksha 'O' Anusandhan University
-   Data Engineering Intern
-   Celebal Technologies Summer Internship 2026
