-- Q24. Write a query using CASE to classify products into price tiers: 
--   • 'Budget'    → unit_price < 1000 
--   • 'Mid-Range' → unit_price BETWEEN 1000 AND 3000 
--   • 'Premium'   → unit_price > 3000 
-- Display: product_name, unit_price, price_tier. 

SELECT
product_name,
unit_price,
CASE
WHEN unit_price<1000 THEN 'Budget'
WHEN unit_price BETWEEN 1000 AND 3000 THEN 'Mid-Range'
ELSE 'Premium'
END AS price_tier
FROM products;



-- Q25. Using a CASE statement inside an aggregate function, count how many orders are 'Delivered' vs 'Not Delivered' (all other statuses). Display the result in a single row. 

SELECT
COUNT(CASE WHEN status='Delivered' THEN 1 END)
AS delivered,

COUNT(CASE WHEN status<>'Delivered' THEN 1 END)
AS not_delivered
FROM orders;

Q26. Explain each letter of ACID: 
  • A – Atomicity --> All operations succeed or all fail.
  • C – Consistency -->The database always remains valid before and after a transaction. Constraints and rules are preserved.
  • I – Isolation  --> Multiple transactions do not interfere with one another. Concurrent transfers should not produce incorrect balances.
  • D – Durability --> Once a transaction is committed, it remains saved permanently, even after a crash or power failure.
Give a real-world example (e.g., bank transfer) showing why each property is important. 

A – Atomicity
Either the entire transaction is completed, or none of it is.

Example: If ₹5,000 is deducted from Account A but the system crashes before adding it to Account B, the transaction is rolled back. The money is returned to Account A.

C – Consistency
The database remains correct before and after the transaction.

Example: Before the transfer, the total money in both accounts is ₹50,000. After transferring ₹5,000, the total is still ₹50,000. No money is lost or created.

I – Isolation
Multiple transactions can occur at the same time without affecting each other.

Example: While Rahul transfers ₹5,000, another customer transfers ₹2,000. Both transactions are processed independently, and neither interferes with the other.

D – Durability
Once a transaction is successfully completed (committed), it is permanently saved.

Example: After Rahul's transfer is successful, the bank server crashes. When the system restarts, the transfer is still recorded, and both account balances remain updated.


-- Q27. Write a SQL transaction that does the following atomically: 
--   1. Insert a new order (order_id=1011, customer_id=102, today's date, 'Pending', 1598.00) 
--   2. Insert two order items for that order 
--   3. Update the stock_qty of the purchased products 
--   4. If any step fails, ROLLBACK the entire transaction. Otherwise, COMMIT. 
-- Write the complete BEGIN...COMMIT/ROLLBACK block.

BEGIN;

INSERT INTO orders
(order_id,customer_id,order_date,status,total_amount)
VALUES
(1011,102,CURRENT_DATE,'Pending',1598.00);

INSERT INTO order_items
(item_id,order_id,product_id,quantity,unit_price,discount_pct)
VALUES
(5016,1011,206,1,1299.00,0);

INSERT INTO order_items
(item_id,order_id,product_id,quantity,unit_price,discount_pct)
VALUES
(5017,1011,208,1,599.00,0);

UPDATE products
SET stock_qty=stock_qty-1
WHERE product_id=206;

UPDATE products
SET stock_qty=stock_qty-1
WHERE product_id=208;

COMMIT;