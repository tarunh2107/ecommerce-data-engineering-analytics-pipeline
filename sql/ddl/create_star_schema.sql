-- =============================================================================
-- DATA WAREHOUSE KIMBALL STAR SCHEMA DDL
-- Database: olist_dw
-- Schema: dwh
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dwh;

-- -----------------------------------------------------------------------------
-- Cleanly drop existing objects in dwh schema if rebuilding
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'dwh') LOOP
        IF r.table_type = 'VIEW' THEN
            EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' CASCADE';
        ELSIF r.table_type = 'BASE TABLE' THEN
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' CASCADE';
        END IF;
    END LOOP;
END $$;

-- -----------------------------------------------------------------------------
-- Dimension: Calendar Date
-- Grain: One row per calendar day
-- -----------------------------------------------------------------------------
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY,               -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(15) NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- -----------------------------------------------------------------------------
-- Dimension: Customers
-- Grain: One row per customer_id
-- -----------------------------------------------------------------------------
CREATE TABLE dwh.dim_customer (
    customer_key INT PRIMARY KEY,           -- Warehouse Surrogate Key
    customer_id VARCHAR(50) UNIQUE NOT NULL, -- Olist Natural Business Key
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Dimension: Products
-- Grain: One row per product_id
-- -----------------------------------------------------------------------------
CREATE TABLE dwh.dim_product (
    product_key INT PRIMARY KEY,            -- Warehouse Surrogate Key
    product_id VARCHAR(50) UNIQUE NOT NULL,  -- Olist Natural Business Key
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g NUMERIC(10, 2),
    product_length_cm NUMERIC(10, 2),
    product_height_cm NUMERIC(10, 2),
    product_width_cm NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Dimension: Sellers
-- Grain: One row per seller_id
-- -----------------------------------------------------------------------------
CREATE TABLE dwh.dim_seller (
    seller_key INT PRIMARY KEY,             -- Warehouse Surrogate Key
    seller_id VARCHAR(50) UNIQUE NOT NULL,   -- Olist Natural Business Key
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Fact: Orders
-- Grain: One row per order (99,441 rows)
-- -----------------------------------------------------------------------------
CREATE TABLE dwh.fact_order (
    order_key INT PRIMARY KEY,              -- Warehouse Surrogate Key
    order_id VARCHAR(50) UNIQUE NOT NULL,   -- Olist Natural Order ID
    customer_key INT NOT NULL REFERENCES dwh.dim_customer(customer_key),
    order_date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    order_approved_date_key INT REFERENCES dwh.dim_date(date_key),
    order_delivered_carrier_date_key INT REFERENCES dwh.dim_date(date_key),
    order_delivered_customer_date_key INT REFERENCES dwh.dim_date(date_key),
    order_estimated_delivery_date_key INT REFERENCES dwh.dim_date(date_key),
    order_status VARCHAR(20) NOT NULL,
    order_item_count INT NOT NULL DEFAULT 0,
    total_price NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    total_freight NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    total_order_value NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    total_payment NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    review_score NUMERIC(3, 2),
    delivery_time_days NUMERIC(6, 2),
    delivery_delay_days NUMERIC(6, 2),
    is_late_delivery BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Fact: Order Items
-- Grain: One row per line item inside an order (112,650 rows)
-- -----------------------------------------------------------------------------
CREATE TABLE dwh.fact_order_item (
    order_item_key INT PRIMARY KEY,         -- Warehouse Surrogate Key
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    customer_key INT NOT NULL REFERENCES dwh.dim_customer(customer_key),
    product_key INT NOT NULL REFERENCES dwh.dim_product(product_key),
    seller_key INT NOT NULL REFERENCES dwh.dim_seller(seller_key),
    order_date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10, 2) NOT NULL,
    freight_value NUMERIC(10, 2) NOT NULL,
    order_item_total NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_order_item UNIQUE (order_id, order_item_id)
);

-- -----------------------------------------------------------------------------
-- Analytical Join Indices
-- -----------------------------------------------------------------------------
CREATE INDEX idx_fact_order_customer_key ON dwh.fact_order(customer_key);
CREATE INDEX idx_fact_order_date_key ON dwh.fact_order(order_date_key);
CREATE INDEX idx_fact_order_status ON dwh.fact_order(order_status);

CREATE INDEX idx_fact_order_item_order_id ON dwh.fact_order_item(order_id);
CREATE INDEX idx_fact_order_item_customer_key ON dwh.fact_order_item(customer_key);
CREATE INDEX idx_fact_order_item_product_key ON dwh.fact_order_item(product_key);
CREATE INDEX idx_fact_order_item_seller_key ON dwh.fact_order_item(seller_key);
CREATE INDEX idx_fact_order_item_date_key ON dwh.fact_order_item(order_date_key);

CREATE INDEX idx_dim_customer_state ON dwh.dim_customer(customer_state);
CREATE INDEX idx_dim_product_category ON dwh.dim_product(product_category_name_english);
CREATE INDEX idx_dim_seller_state ON dwh.dim_seller(seller_state);

-- -----------------------------------------------------------------------------
-- Compatibility Views (Supports plural naming convention queries)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dwh.dim_customers AS SELECT * FROM dwh.dim_customer;
CREATE OR REPLACE VIEW dwh.dim_products AS SELECT * FROM dwh.dim_product;
CREATE OR REPLACE VIEW dwh.dim_sellers AS SELECT * FROM dwh.dim_seller;
CREATE OR REPLACE VIEW dwh.fact_orders AS SELECT * FROM dwh.fact_order;
CREATE OR REPLACE VIEW dwh.fact_order_items AS SELECT * FROM dwh.fact_order_item;
