
CREATE DATABASE airflow;


CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    full_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name       VARCHAR(100) UNIQUE NOT NULL,
    price      NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers (customer_id),
    product_name VARCHAR(100) NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    quantity     INT NOT NULL,
    status       VARCHAR(20) DEFAULT 'completed',
    created_at   TIMESTAMP DEFAULT NOW()
);


INSERT INTO customers (full_name, email)
SELECT 'Customer ' || g, 'customer' || g || '@example.com'
FROM generate_series(1, 50) AS g;

INSERT INTO products (name, price) VALUES
    ('Laptop', 450.00), ('Mouse', 15.00), ('Keyboard', 30.00),
    ('Monitor', 150.00), ('Headphones', 60.00), ('USB Cable', 5.00);
