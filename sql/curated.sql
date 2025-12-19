-- =========================
-- CURATED LAYER
-- =========================

CREATE DATABASE IF NOT EXISTS CURATED_DB;
CREATE SCHEMA IF NOT EXISTS CURATED_DB.DATA;

-- Curated Tables
CREATE OR REPLACE TABLE CURATED_DB.DATA.customers (
    customer_id INT PRIMARY KEY,
    full_name   STRING,
    email       STRING,
    country     STRING,
    signup_date DATE,
    status      STRING,
    is_active   BOOLEAN
);

CREATE OR REPLACE TABLE CURATED_DB.DATA.orders (
    order_id    INT PRIMARY KEY,
    customer_id INT,
    product     STRING,
    quantity    INT,
    amount      NUMBER(10,2),
    tax         NUMBER(10,2),
    total       NUMBER(10,2),
    order_date  DATE,
    status      STRING
);

-- Transform Customers
TRUNCATE TABLE CURATED_DB.DATA.customers;

INSERT INTO CURATED_DB.DATA.customers
SELECT
    customer_id,
    first_name || ' ' || last_name AS full_name,
    email,
    country,
    signup_date,
    status,
    status = 'Active' AS is_active
FROM RAW_DB.STAGE.customers_raw;

-- Transform Orders
TRUNCATE TABLE CURATED_DB.DATA.orders;

INSERT INTO CURATED_DB.DATA.orders
SELECT
    order_id,
    customer_id,
    product,
    quantity,
    amount,
    amount * 0.10 AS tax,
    amount * 1.10 AS total,
    order_date,
    status
FROM RAW_DB.STAGE.orders_raw;
