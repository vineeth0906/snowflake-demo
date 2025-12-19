-- =========================
-- RAW LAYER DEPLOYMENT
-- =========================

-- Databases & Schemas
CREATE DATABASE IF NOT EXISTS RAW_DB;
CREATE SCHEMA IF NOT EXISTS RAW_DB.STAGE;

-- Raw Tables
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

-- Load Data
TRUNCATE TABLE RAW_DB.STAGE.customers_raw;

COPY INTO RAW_DB.STAGE.customers_raw
FROM @~/customers.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

TRUNCATE TABLE RAW_DB.STAGE.orders_raw;

COPY INTO RAW_DB.STAGE.orders_raw
FROM @~/orders.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
