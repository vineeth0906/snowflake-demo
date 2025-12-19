-- =========================
-- PUBLISH / ANALYTICS LAYER
-- =========================

CREATE DATABASE IF NOT EXISTS PUBLISH_DB;
CREATE SCHEMA IF NOT EXISTS PUBLISH_DB.ANALYTICS;

-- Tables
CREATE OR REPLACE TABLE PUBLISH_DB.ANALYTICS.customer_summary (
    customer_id      INT,
    full_name        STRING,
    country          STRING,
    total_orders     INT,
    total_spent      NUMBER(15,2),
    avg_order_value  NUMBER(10,2),
    customer_segment STRING
);

CREATE OR REPLACE TABLE PUBLISH_DB.ANALYTICS.daily_sales (
    order_date     DATE,
    total_orders   INT,
    total_revenue  NUMBER(15,2),
    avg_order_value NUMBER(10,2)
);

-- Customer Summary
TRUNCATE TABLE PUBLISH_DB.ANALYTICS.customer_summary;

INSERT INTO PUBLISH_DB.ANALYTICS.customer_summary
SELECT
    c.customer_id,
    c.full_name,
    c.country,
    COUNT(o.order_id),
    SUM(o.total),
    AVG(o.total),
    CASE
        WHEN SUM(o.total) > 10000 THEN 'VIP'
        WHEN SUM(o.total) > 3000 THEN 'REGULAR'
        ELSE 'NEW'
    END
FROM CURATED_DB.DATA.customers c
LEFT JOIN CURATED_DB.DATA.orders o
ON c.customer_id = o.customer_id
GROUP BY 1,2,3;

-- Daily Sales
TRUNCATE TABLE PUBLISH_DB.ANALYTICS.daily_sales;

INSERT INTO PUBLISH_DB.ANALYTICS.daily_sales
SELECT
    order_date,
    COUNT(*),
    SUM(total),
    AVG(total)
FROM CURATED_DB.DATA.orders
GROUP BY order_date;
