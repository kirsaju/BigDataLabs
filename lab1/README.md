# Lab 1 - Snowflake Schema (Pet Supply Store)

## How to Run

```bash
cd lab1
docker-compose up -d
```

This starts a PostgreSQL 16 container that automatically:
1. Creates the `raw_data` staging table
2. Loads all 10 CSV files (10,000 rows total)
3. Creates the snowflake schema (fact + dimension + sub-dimension tables)
4. Populates all snowflake tables from raw data

## How to Verify

Connect to the database:

```bash
docker exec -it lab1_postgres psql -U lab -d petstore
```

Check row counts:

```sql
SELECT 'raw_data' AS tbl, COUNT(*) FROM raw_data
UNION ALL SELECT 'f_sale', COUNT(*) FROM f_sale
UNION ALL SELECT 'd_customer', COUNT(*) FROM d_customer
UNION ALL SELECT 'd_seller', COUNT(*) FROM d_seller
UNION ALL SELECT 'd_product', COUNT(*) FROM d_product
UNION ALL SELECT 'd_store', COUNT(*) FROM d_store
UNION ALL SELECT 'd_supplier', COUNT(*) FROM d_supplier
UNION ALL SELECT 'd_date', COUNT(*) FROM d_date;
```

## Schema Overview

```
f_sale (fact)
  -> d_customer   -> d_customer_location, d_customer_pet
  -> d_seller     -> d_seller_location
  -> d_product    -> d_product_category, d_product_details
  -> d_store      -> d_store_location
  -> d_supplier   -> d_supplier_location
  -> d_date
```

## Teardown

```bash
docker-compose down -v
```
