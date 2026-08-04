-- =====================================================================
-- Part 3: SQL Analysis
-- Run against db/ecommerce.db (SQLite)
-- revenue = quantity * unit_price * (1 - discount_percent/100)
-- =====================================================================


-- ---------------------------------------------------------------------
-- BASIC QUERIES
-- ---------------------------------------------------------------------

-- 1. Total revenue per category
SELECT
    p.category,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;


-- 2. Top 10 customers by total order value
SELECT
    c.customer_id,
    c.customer_name,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_order_value
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_order_value DESC
LIMIT 10;


-- 3. Month-wise order count for the last 12 months
SELECT
    strftime('%Y-%m', order_date) AS order_month,
    COUNT(*) AS order_count
FROM orders
WHERE order_date >= date((SELECT MAX(order_date) FROM orders), '-12 months')
GROUP BY order_month
ORDER BY order_month;


-- ---------------------------------------------------------------------
-- INTERMEDIATE QUERIES
-- ---------------------------------------------------------------------

-- 4. Customers who placed orders but never had any item delivered
SELECT DISTINCT c.customer_id, c.customer_name
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE c.customer_id NOT IN (
    SELECT DISTINCT customer_id FROM orders WHERE status = 'DELIVERED' AND customer_id IS NOT NULL
);


-- 5. Products that were ordered but had more returns than purchases
-- (a "return" = a row with negative quantity; "purchase" = positive quantity)
SELECT
    p.product_id,
    p.product_name,
    SUM(CASE WHEN oi.quantity > 0 THEN oi.quantity ELSE 0 END) AS total_purchased,
    SUM(CASE WHEN oi.quantity < 0 THEN -oi.quantity ELSE 0 END) AS total_returned
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
HAVING total_returned > total_purchased;


-- 6. Return rate (returned items / total items) per category
SELECT
    p.category,
    SUM(CASE WHEN oi.quantity < 0 THEN -oi.quantity ELSE 0 END) AS returned_items,
    SUM(ABS(oi.quantity)) AS total_items,
    ROUND(
        SUM(CASE WHEN oi.quantity < 0 THEN -oi.quantity ELSE 0 END) * 100.0
        / NULLIF(SUM(ABS(oi.quantity)), 0), 2
    ) AS return_rate_percent
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY return_rate_percent DESC;


-- ---------------------------------------------------------------------
-- ADVANCED QUERIES (Window Functions, CTEs, Subqueries)
-- ---------------------------------------------------------------------

-- 7. Running totals with window functions
-- Show: region_code, order_date, daily_revenue, running_total
WITH daily_region_revenue AS (
    SELECT
        o.region_code,
        date(o.order_date) AS order_date,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS daily_revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY o.region_code, date(o.order_date)
)
SELECT
    region_code,
    order_date,
    ROUND(daily_revenue, 2) AS daily_revenue,
    ROUND(SUM(daily_revenue) OVER (
        PARTITION BY region_code ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS running_total
FROM daily_region_revenue
ORDER BY region_code, order_date;


-- 8. Ranking with DENSE_RANK
-- Show: category, product_name, total_revenue, rank_in_category
WITH product_revenue AS (
    SELECT
        p.category,
        p.product_name,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS total_revenue
    FROM order_items oi
    JOIN products p ON p.product_id = oi.product_id
    GROUP BY p.category, p.product_name
)
SELECT
    category,
    product_name,
    ROUND(total_revenue, 2) AS total_revenue,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rank_in_category
FROM product_revenue
ORDER BY category, rank_in_category;


-- 9. LAG/LEAD analysis
-- Show: customer_id, order_date, previous_order_date, days_gap
-- Flag customers with average gap > 30 days as "At Risk"
WITH customer_orders AS (
    SELECT
        customer_id,
        date(order_date) AS order_date,
        LAG(date(order_date)) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date
    FROM orders
    WHERE customer_id IS NOT NULL
),
gaps AS (
    SELECT
        customer_id,
        order_date,
        previous_order_date,
        CASE WHEN previous_order_date IS NOT NULL
             THEN julianday(order_date) - julianday(previous_order_date)
             ELSE NULL END AS days_gap
    FROM customer_orders
)
SELECT
    g.customer_id,
    g.order_date,
    g.previous_order_date,
    g.days_gap,
    CASE WHEN avg_gap.avg_days_gap > 30 THEN 'At Risk' ELSE 'OK' END AS risk_flag
FROM gaps g
JOIN (
    SELECT customer_id, AVG(days_gap) AS avg_days_gap
    FROM gaps
    WHERE days_gap IS NOT NULL
    GROUP BY customer_id
) avg_gap ON avg_gap.customer_id = g.customer_id
ORDER BY g.customer_id, g.order_date;


-- 10. CTE with multiple levels
-- Step 1: monthly revenue per customer
-- Step 2: categorize customers High/Medium/Low
-- Step 3: count of customers in each category per month
WITH monthly_customer_revenue AS (
    SELECT
        o.customer_id,
        strftime('%Y-%m', o.order_date) AS order_month,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS monthly_revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.customer_id IS NOT NULL
    GROUP BY o.customer_id, order_month
),
categorized AS (
    SELECT
        customer_id,
        order_month,
        monthly_revenue,
        CASE
            WHEN monthly_revenue > 10000 THEN 'High'
            WHEN monthly_revenue >= 5000 THEN 'Medium'
            ELSE 'Low'
        END AS revenue_category
    FROM monthly_customer_revenue
)
SELECT
    order_month,
    revenue_category,
    COUNT(DISTINCT customer_id) AS customer_count
FROM categorized
GROUP BY order_month, revenue_category
ORDER BY order_month, revenue_category;


-- 11. NTILE for segmentation
-- Show: customer_id, total_value, quartile, quartile_label
WITH customer_ltv AS (
    SELECT
        o.customer_id,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS total_value
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.customer_id IS NOT NULL
    GROUP BY o.customer_id
)
SELECT
    customer_id,
    ROUND(total_value, 2) AS total_value,
    NTILE(4) OVER (ORDER BY total_value DESC) AS quartile,
    CASE NTILE(4) OVER (ORDER BY total_value DESC)
        WHEN 1 THEN 'Platinum'
        WHEN 2 THEN 'Gold'
        WHEN 3 THEN 'Silver'
        WHEN 4 THEN 'Bronze'
    END AS quartile_label
FROM customer_ltv
ORDER BY total_value DESC;


-- 12. Year-over-year comparison
-- Show: year, month, revenue, prev_year_revenue, yoy_growth_percent
WITH monthly_revenue AS (
    SELECT
        CAST(strftime('%Y', o.order_date) AS INTEGER) AS year,
        CAST(strftime('%m', o.order_date) AS INTEGER) AS month,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY year, month
)
SELECT
    year,
    month,
    ROUND(revenue, 2) AS revenue,
    ROUND(LAG(revenue) OVER (PARTITION BY month ORDER BY year), 2) AS prev_year_revenue,
    CASE
        WHEN LAG(revenue) OVER (PARTITION BY month ORDER BY year) IS NULL THEN NULL
        WHEN LAG(revenue) OVER (PARTITION BY month ORDER BY year) = 0 THEN NULL
        ELSE ROUND(
            (revenue - LAG(revenue) OVER (PARTITION BY month ORDER BY year)) * 100.0
            / LAG(revenue) OVER (PARTITION BY month ORDER BY year), 2
        )
    END AS yoy_growth_percent
FROM monthly_revenue
ORDER BY year, month;


-- 13. First/Last value analysis
-- For each customer, first purchased category vs most recent purchased category
WITH customer_category_orders AS (
    SELECT
        o.customer_id,
        o.order_date,
        p.category,
        FIRST_VALUE(p.category) OVER (
            PARTITION BY o.customer_id ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_category,
        LAST_VALUE(p.category) OVER (
            PARTITION BY o.customer_id ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_category
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p ON p.product_id = oi.product_id
    WHERE o.customer_id IS NOT NULL
)
SELECT DISTINCT
    customer_id,
    first_category,
    last_category,
    CASE WHEN first_category != last_category THEN 'Yes' ELSE 'No' END AS category_shift
FROM customer_category_orders
ORDER BY customer_id;


-- 14. Cumulative distribution
-- Show: customer_id, revenue, cumulative_revenue, cumulative_percent
WITH customer_revenue AS (
    SELECT
        o.customer_id,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.customer_id IS NOT NULL
    GROUP BY o.customer_id
),
ranked AS (
    SELECT
        customer_id,
        revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
        SUM(revenue) OVER () AS total_revenue
    FROM customer_revenue
)
SELECT
    customer_id,
    ROUND(revenue, 2) AS revenue,
    ROUND(cumulative_revenue, 2) AS cumulative_revenue,
    ROUND(cumulative_revenue * 100.0 / total_revenue, 2) AS cumulative_percent
FROM ranked
ORDER BY revenue DESC;


-- 15. Complex CTE: Cohort analysis
-- Group customers by registration month; retention in month 0/1/2/3
WITH cohorts AS (
    SELECT
        customer_id,
        strftime('%Y-%m', registration_date) AS cohort_month
    FROM customers
),
customer_order_months AS (
    SELECT DISTINCT
        customer_id,
        strftime('%Y-%m', order_date) AS order_month
    FROM orders
    WHERE customer_id IS NOT NULL
),
cohort_activity AS (
    SELECT
        c.customer_id,
        c.cohort_month,
        co.order_month,
        CAST(
            (CAST(strftime('%Y', co.order_month || '-01') AS INTEGER) - CAST(strftime('%Y', c.cohort_month || '-01') AS INTEGER)) * 12
            + (CAST(strftime('%m', co.order_month || '-01') AS INTEGER) - CAST(strftime('%m', c.cohort_month || '-01') AS INTEGER))
            AS INTEGER
        ) AS month_number
    FROM cohorts c
    JOIN customer_order_months co ON co.customer_id = c.customer_id
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM cohorts
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    cs.cohort_size,
    SUM(CASE WHEN ca.month_number = 0 THEN 1 ELSE 0 END) AS active_month_0,
    SUM(CASE WHEN ca.month_number = 1 THEN 1 ELSE 0 END) AS active_month_1,
    SUM(CASE WHEN ca.month_number = 2 THEN 1 ELSE 0 END) AS active_month_2,
    SUM(CASE WHEN ca.month_number = 3 THEN 1 ELSE 0 END) AS active_month_3,
    ROUND(SUM(CASE WHEN ca.month_number = 0 THEN 1 ELSE 0 END) * 100.0 / cs.cohort_size, 2) AS retention_month_0_pct,
    ROUND(SUM(CASE WHEN ca.month_number = 1 THEN 1 ELSE 0 END) * 100.0 / cs.cohort_size, 2) AS retention_month_1_pct,
    ROUND(SUM(CASE WHEN ca.month_number = 2 THEN 1 ELSE 0 END) * 100.0 / cs.cohort_size, 2) AS retention_month_2_pct,
    ROUND(SUM(CASE WHEN ca.month_number = 3 THEN 1 ELSE 0 END) * 100.0 / cs.cohort_size, 2) AS retention_month_3_pct
FROM cohort_activity ca
JOIN cohort_sizes cs ON cs.cohort_month = ca.cohort_month
GROUP BY ca.cohort_month, cs.cohort_size
ORDER BY ca.cohort_month;


-- 16. Self-join with window function
-- Products frequently bought together (same order); exclude duplicate/reverse pairs
WITH pairs AS (
    SELECT
        oi1.product_id AS product_a_id,
        oi2.product_id AS product_b_id,
        oi1.order_id
    FROM order_items oi1
    JOIN order_items oi2
        ON oi1.order_id = oi2.order_id
        AND oi1.product_id < oi2.product_id   -- ensures A-B only once, never B-A duplicate
)
SELECT
    pa.product_name AS product_a,
    pb.product_name AS product_b,
    COUNT(DISTINCT p.order_id) AS times_bought_together
FROM pairs p
JOIN products pa ON pa.product_id = p.product_a_id
JOIN products pb ON pb.product_id = p.product_b_id
GROUP BY pa.product_name, pb.product_name
ORDER BY times_bought_together DESC
LIMIT 20;
