--Q1. Write a query to display all columns and rows from the customer's table.
  
  SELECT * FROM customers;

  
--Q2. Retrieve only the first_name, last_name, and city of all customers. 

   SELECT first_name, last_name, city
   FROM customers;

--Q3. List all unique categories available in the products table. 

  SELECT DISTINCT category
  FROM products;

 
--Q4. Identify the Primary Key of each table in the schema. Explain why a Primary Key must be unique and NOT NULL. 

ANS-> Table          Primary Key 
      customers      customer_id 
      products       product_id  
      orders         order_id    
      order_items    item_id     

Primary Key uniquely identifies each record.
Cannot contain NULL values.
Prevents duplicate records.


--Q5. What constraints are applied to the email column in the customers table? What would happen if you tried to insert a duplicate email? 

ANS-> NOT NULL
      UNIQUE

INSERT INTO customers
VALUES
(109,'Rahul','Kumar','aarav.s@email.com',
'Patna','Bihar','2024-09-01',TRUE);

RESULT:
ERROR:  duplicate key value violates unique constraint "customers_email_key"
Key (email)=(aarav.s@email.com) already exists. 

SQL state: 23505
Detail: Key (email)=(aarav.s@email.com) already exists.

	  
--Q6. Try inserting a product with unit_price = -50. What happens and which constraint prevents it? Write both the INSERT statement and explain the error.

INSERT INTO products
VALUES
(209,'Test Product','Electronics',
'Test',-50,100);

RESULT

ERROR:  new row for relation "products" violates check constraint "products_unit_price_check"
Failing row contains (209, Test Product, Electronics, Test, -50.00, 100). 

SQL state: 23514
Detail: Failing row contains (209, Test Product, Electronics, Test, -50.00, 100).
