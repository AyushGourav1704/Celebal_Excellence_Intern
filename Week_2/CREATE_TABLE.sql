-- Customers table

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    join_date DATE NOT NULL,
    is_premium BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_customers_city
ON customers(city);

CREATE INDEX idx_customers_state
ON customers(state);

-- Products table

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    brand VARCHAR(50) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    stock_qty INT NOT NULL DEFAULT 0 CHECK (stock_qty >= 0)
);


CREATE INDEX idx_products_category
ON products(category);

-- Order table

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Pending',
    total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),

    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id),

    CHECK (status IN ('Pending','Shipped','Delivered','Cancelled'))
);


CREATE INDEX idx_orders_date
ON orders(order_date);

CREATE INDEX idx_orders_status
ON orders(status);

-- Order_items

CREATE TABLE order_items (
    item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    discount_pct DECIMAL(5,2) DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 100),

    FOREIGN KEY (order_id)
        REFERENCES orders(order_id),

    FOREIGN KEY (product_id)
        REFERENCES products(product_id)
);


SELECT table_name
FROM information_schema.tables
WHERE table_schema='public';

