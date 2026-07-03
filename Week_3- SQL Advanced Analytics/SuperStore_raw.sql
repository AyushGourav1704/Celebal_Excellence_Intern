--                               STEP-1(SETUP DATA)

--customers Table

CREATE TABLE customers AS
SELECT DISTINCT
    "Customer ID",
    "Customer Name",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region"
FROM superstore_raw;

--products Table

CREATE TABLE products AS
SELECT DISTINCT
    "Product ID",
    "Category",
    "Sub-Category",
    "Product Name"
FROM superstore_raw;

--orders Table

CREATE TABLE orders AS
SELECT DISTINCT
    "Order ID",
    "Order Date",
    "Ship Date",
    "Ship Mode",
    "Customer ID",
    "Product ID",
    "Sales",
    "Quantity",
    "Discount",
    "Profit"
FROM superstore_raw;

--                         STEP-2(PERFORM REQUIRED QUERIES)


--Q1.Find all orders where sales are greater than the average sales. (Subquery)  

SELECT *
FROM orders
WHERE "Sales" > (
    SELECT AVG("Sales")
    FROM orders
);

--Q2.Find the highest sales order for each customer. (Subquery)  

SELECT *
FROM orders o
WHERE "Sales" =
(
    SELECT MAX(o2."Sales")
    FROM orders o2
    WHERE o2."Customer ID" = o."Customer ID"
);

--Q3.Calculate total sales for each customer (CTE)

WITH customer_sales AS
(
    SELECT
        "Customer ID",
        SUM("Sales") AS total_sales
    FROM orders
    GROUP BY "Customer ID"
)

SELECT *
FROM customer_sales
ORDER BY total_sales DESC;

--4. Find customers whose total sales are above average (CTE + Subquery)

WITH customer_sales AS
(
    SELECT
        "Customer ID",
        SUM("Sales") AS total_sales
    FROM orders
    GROUP BY "Customer ID"
)

SELECT *
FROM customer_sales
WHERE total_sales >
(
    SELECT AVG(total_sales)
    FROM customer_sales
);


--5. Rank all customers based on total sales (Window Function)

WITH customer_sales AS
(
    SELECT
        "Customer ID",
        SUM("Sales") AS total_sales
    FROM orders
    GROUP BY "Customer ID"
)

SELECT
    "Customer ID",
    total_sales,
    RANK() OVER(ORDER BY total_sales DESC) AS customer_rank
FROM customer_sales;

--Q6.Assign row numbers to each order within a customer (Window Function + PARTITION BY)


SELECT
    "Customer ID",
    "Order ID",
    "Sales",

    ROW_NUMBER() OVER(
        PARTITION BY "Customer ID"
        ORDER BY "Sales" DESC
    ) AS row_number

FROM orders;


--Q7 Display Top 3 customers based on total sales (Window Function)

WITH customer_sales AS
(
    SELECT
        "Customer ID",
        SUM("Sales") AS total_sales
    FROM orders
    GROUP BY "Customer ID"
),

ranked_customers AS
(
    SELECT
        "Customer ID",
        total_sales,
        RANK() OVER(ORDER BY total_sales DESC) AS customer_rank
    FROM customer_sales
)

SELECT *
FROM ranked_customers
WHERE customer_rank <= 3;


--                         STEP-3 (FINAL COMBINED QUERY)

WITH customer_sales AS
(
    SELECT
        "Customer ID",
        SUM("Sales") AS total_sales
    FROM orders
    GROUP BY "Customer ID"
)

SELECT

    c."Customer Name",

    cs.total_sales,

    RANK() OVER(
        ORDER BY cs.total_sales DESC
    ) AS customer_rank

FROM customer_sales cs

JOIN customers c
ON cs."Customer ID" = c."Customer ID"

ORDER BY customer_rank;


--                    Mini Project: Customer Sales Insights 

--1. Who are the Top 5 customers?

SELECT
    "Customer Name",
    SUM("Sales") AS total_sales
FROM superstore_raw
GROUP BY "Customer Name"
ORDER BY total_sales DESC
LIMIT 5;

--2. Who are the Bottom 5 customers?

SELECT
    "Customer Name",
    SUM("Sales") AS total_sales
FROM superstore_raw
GROUP BY "Customer Name"
ORDER BY total_sales ASC
LIMIT 5;


--3. Which customers made only one order?

SELECT
    "Customer Name",
    COUNT(DISTINCT "Order ID") AS total_orders
FROM superstore_raw
GROUP BY "Customer Name"
HAVING COUNT(DISTINCT "Order ID") = 1;

--4. Which customers have above-average sales?

WITH customer_sales AS
(
    SELECT
        "Customer Name",
        SUM("Sales") AS total_sales
    FROM superstore_raw
    GROUP BY "Customer Name"
)

SELECT *
FROM customer_sales
WHERE total_sales >
(
    SELECT AVG(total_sales)
    FROM customer_sales
);


--5. What is the highest order value per customer?

SELECT
    "Customer Name",
    MAX("Sales") AS highest_order_value
FROM superstore_raw
GROUP BY "Customer Name"
ORDER BY highest_order_value DESC;

