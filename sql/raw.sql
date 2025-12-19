CREATE DATABASE IF NOT EXISTS RAW_DB;
CREATE SCHEMA IF NOT EXISTS RAW_DB.STAGE;

CREATE OR REPLACE TABLE RAW_DB.STAGE.customers_raw (
    customer_id  INT,
    first_name   STRING,
    last_name    STRING,
    email        STRING,
    country      STRING,
    signup_date  DATE,
    status       STRING,
    load_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_DB.STAGE.orders_raw (
    order_id    INT,
    customer_id INT,
    product     STRING,
    quantity    INT,
    amount      NUMBER(10,2),
    order_date  DATE,
    status      STRING,
    load_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
