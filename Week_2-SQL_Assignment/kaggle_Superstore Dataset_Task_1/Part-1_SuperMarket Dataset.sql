--. Validate Row Count

SELECT COUNT(*) AS total_rows
FROM superstore;

-- Check NULL Values

SELECT
COUNT(*) FILTER (WHERE "Order ID" IS NULL) AS order_id_null,
COUNT(*) FILTER (WHERE "Customer Name" IS NULL) AS customer_name_null,
COUNT(*) FILTER (WHERE sales IS NULL) AS sales_null,
COUNT(*) FILTER (WHERE profit IS NULL) AS profit_null
FROM superstore;

-- Find Duplicate Order IDs

SELECT "Order ID",
       COUNT(*) AS duplicate_count
FROM superstore
GROUP BY "Order ID"
HAVING COUNT(*) > 1
order by duplicate_count  DESC;

SELECT *
FROM superstore
WHERE region = 'West';

SELECT *
FROM superstore
WHERE category = 'Technology';

SELECT *
FROM superstore
WHERE sales > 500;


SELECT *
FROM superstore
WHERE TO_DATE("Order Date",'MM/DD/YYYY')
BETWEEN '2017-01-01' AND '2017-12-31';

SELECT
region,
SUM(sales) AS total_sales
FROM superstore
GROUP BY region
ORDER BY total_sales DESC;

SELECT
category,
SUM(profit) AS total_profit
FROM superstore
GROUP BY category
ORDER BY total_profit DESC;

SELECT
segment,
AVG(sales) AS average_sales
FROM superstore
GROUP BY segment;

SELECT
SUM(quantity) AS total_quantity
FROM superstore;

SELECT
"Customer Name",
SUM(sales) AS total_sales
FROM superstore
GROUP BY "Customer Name"
ORDER BY total_sales DESC
LIMIT 10;

SELECT
"Product Name",
SUM(sales) AS total_sales
FROM superstore
GROUP BY "Product Name"
ORDER BY total_sales DESC
LIMIT 10;

SELECT
category,
SUM(sales) AS total_sales
FROM superstore
GROUP BY category
ORDER BY total_sales DESC;

SELECT
"Product Name",
SUM(profit) AS total_profit
FROM superstore
GROUP BY "Product Name"
ORDER BY total_profit DESC
LIMIT 10;

SELECT
DATE_TRUNC(
'month',
TO_DATE("Order Date",'MM/DD/YYYY')
) AS month,
SUM(sales) AS total_sales
FROM superstore
GROUP BY month
ORDER BY month;

SELECT
region,
SUM(profit) AS total_profit
FROM superstore
GROUP BY region
ORDER BY total_profit DESC;

SELECT
category,
AVG(discount) AS average_discount
FROM superstore
GROUP BY category;