-- 03_create_snowflake.sql
-- Snowflake schema DDL for pet supply store data warehouse.
-- Sub-dimensions (second level) are created first, then first-level dimensions, then fact table.

-- ============================================================
-- SECOND-LEVEL SUB-DIMENSION TABLES
-- ============================================================

DROP TABLE IF EXISTS f_sale CASCADE;
DROP TABLE IF EXISTS d_customer CASCADE;
DROP TABLE IF EXISTS d_seller CASCADE;
DROP TABLE IF EXISTS d_product CASCADE;
DROP TABLE IF EXISTS d_store CASCADE;
DROP TABLE IF EXISTS d_supplier CASCADE;
DROP TABLE IF EXISTS d_date CASCADE;
DROP TABLE IF EXISTS d_customer_location CASCADE;
DROP TABLE IF EXISTS d_customer_pet CASCADE;
DROP TABLE IF EXISTS d_seller_location CASCADE;
DROP TABLE IF EXISTS d_store_location CASCADE;
DROP TABLE IF EXISTS d_supplier_location CASCADE;
DROP TABLE IF EXISTS d_product_category CASCADE;
DROP TABLE IF EXISTS d_product_details CASCADE;

-- Customer location sub-dimension
CREATE TABLE d_customer_location (
    location_id   SERIAL PRIMARY KEY,
    country       VARCHAR(200),
    postal_code   VARCHAR(50)
);

-- Customer pet sub-dimension
CREATE TABLE d_customer_pet (
    pet_id        SERIAL PRIMARY KEY,
    pet_type      VARCHAR(100),
    pet_name      VARCHAR(200),
    pet_breed     VARCHAR(200),
    pet_category  VARCHAR(100)
);

-- Seller location sub-dimension
CREATE TABLE d_seller_location (
    location_id   SERIAL PRIMARY KEY,
    country       VARCHAR(200),
    postal_code   VARCHAR(50)
);

-- Store location sub-dimension
CREATE TABLE d_store_location (
    location_id   SERIAL PRIMARY KEY,
    city          VARCHAR(200),
    state         VARCHAR(200),
    country       VARCHAR(200)
);

-- Supplier location sub-dimension
CREATE TABLE d_supplier_location (
    location_id   SERIAL PRIMARY KEY,
    address       VARCHAR(300),
    city          VARCHAR(200),
    country       VARCHAR(200)
);

-- Product category sub-dimension
CREATE TABLE d_product_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(200)
);

-- Product details sub-dimension
CREATE TABLE d_product_details (
    details_id    SERIAL PRIMARY KEY,
    weight        NUMERIC(10,2),
    color         VARCHAR(100),
    size          VARCHAR(50),
    brand         VARCHAR(200),
    material      VARCHAR(200),
    description   TEXT,
    rating        NUMERIC(3,1),
    reviews       INT,
    release_date  DATE,
    expiry_date   DATE
);

-- ============================================================
-- FIRST-LEVEL DIMENSION TABLES
-- ============================================================

-- Customer dimension
CREATE TABLE d_customer (
    customer_id   SERIAL PRIMARY KEY,
    first_name    VARCHAR(200),
    last_name     VARCHAR(200),
    age           INT,
    email         VARCHAR(300),
    pet_id        INT REFERENCES d_customer_pet(pet_id),
    location_id   INT REFERENCES d_customer_location(location_id)
);

-- Seller dimension
CREATE TABLE d_seller (
    seller_id     SERIAL PRIMARY KEY,
    first_name    VARCHAR(200),
    last_name     VARCHAR(200),
    email         VARCHAR(300),
    location_id   INT REFERENCES d_seller_location(location_id)
);

-- Product dimension
CREATE TABLE d_product (
    product_id    SERIAL PRIMARY KEY,
    name          VARCHAR(300),
    price         NUMERIC(10,2),
    quantity      INT,
    category_id   INT REFERENCES d_product_category(category_id),
    details_id    INT REFERENCES d_product_details(details_id)
);

-- Store dimension
CREATE TABLE d_store (
    store_id      SERIAL PRIMARY KEY,
    name          VARCHAR(300),
    location_detail VARCHAR(300),
    phone         VARCHAR(50),
    email         VARCHAR(300),
    location_id   INT REFERENCES d_store_location(location_id)
);

-- Supplier dimension
CREATE TABLE d_supplier (
    supplier_id   SERIAL PRIMARY KEY,
    name          VARCHAR(300),
    contact       VARCHAR(300),
    email         VARCHAR(300),
    phone         VARCHAR(50),
    location_id   INT REFERENCES d_supplier_location(location_id)
);

-- Date dimension
CREATE TABLE d_date (
    date_id       SERIAL PRIMARY KEY,
    full_date     DATE UNIQUE,
    day           INT,
    month         INT,
    year          INT,
    quarter       INT
);

-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE f_sale (
    sale_id       SERIAL PRIMARY KEY,
    sale_quantity  INT,
    sale_total_price NUMERIC(12,2),
    customer_id   INT REFERENCES d_customer(customer_id),
    seller_id     INT REFERENCES d_seller(seller_id),
    product_id    INT REFERENCES d_product(product_id),
    store_id      INT REFERENCES d_store(store_id),
    supplier_id   INT REFERENCES d_supplier(supplier_id),
    date_id       INT REFERENCES d_date(date_id)
);

-- ============================================================
-- INDEXES for optimized queries
-- ============================================================

-- Fact table foreign key indexes
CREATE INDEX idx_f_sale_customer_id  ON f_sale(customer_id);
CREATE INDEX idx_f_sale_seller_id    ON f_sale(seller_id);
CREATE INDEX idx_f_sale_product_id   ON f_sale(product_id);
CREATE INDEX idx_f_sale_store_id     ON f_sale(store_id);
CREATE INDEX idx_f_sale_supplier_id  ON f_sale(supplier_id);
CREATE INDEX idx_f_sale_date_id      ON f_sale(date_id);

-- First-level dimension FK indexes
CREATE INDEX idx_d_customer_pet_id       ON d_customer(pet_id);
CREATE INDEX idx_d_customer_location_id  ON d_customer(location_id);
CREATE INDEX idx_d_seller_location_id    ON d_seller(location_id);
CREATE INDEX idx_d_product_category_id   ON d_product(category_id);
CREATE INDEX idx_d_product_details_id    ON d_product(details_id);
CREATE INDEX idx_d_store_location_id     ON d_store(location_id);
CREATE INDEX idx_d_supplier_location_id  ON d_supplier(location_id);

-- Date dimension lookup index
CREATE INDEX idx_d_date_full_date ON d_date(full_date);

-- Deduplication / join helper indexes on natural keys
CREATE INDEX idx_d_customer_name_email ON d_customer(first_name, last_name, email);
CREATE INDEX idx_d_seller_name_email   ON d_seller(first_name, last_name, email);
CREATE INDEX idx_d_product_name_cat    ON d_product(name, category_id);
CREATE INDEX idx_d_store_name_city     ON d_store(name, location_id);
CREATE INDEX idx_d_supplier_name_email ON d_supplier(name, email);
