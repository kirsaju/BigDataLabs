# Lab 2 - Spark ETL Pipeline (Star Schema)

## Architecture

Raw CSV data -> PostgreSQL (mock_data) -> Spark ETL -> PostgreSQL (star schema) -> Spark ETL -> ClickHouse (6 analytical marts)

**Star schema**: fact_sales + 6 dimension tables (dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date).

## How to run

```bash
cd lab2
docker-compose up --build
```

The pipeline runs automatically:
1. PostgreSQL initializes and loads 10 CSV files (10,000 rows total) into `mock_data`
2. Spark runs `etl_to_star.py` -- transforms raw data into a star schema in PostgreSQL
3. Spark runs `etl_to_marts.py` -- reads star schema, builds 6 analytical marts, writes to ClickHouse

## Verify results

PostgreSQL (star schema):
```bash
docker exec -it postgres psql -U my_user -d bigdata_lab2 -c "SELECT tablename FROM pg_tables WHERE schemaname='public';"
docker exec -it postgres psql -U my_user -d bigdata_lab2 -c "SELECT count(*) FROM fact_sales;"
```

ClickHouse (marts):
```bash
docker exec -it clickhouse clickhouse-client --user click --password click --query "SHOW TABLES FROM default;"
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT * FROM default.mart_product_sales LIMIT 10;"
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT * FROM default.mart_time_sales ORDER BY year, month;"
```

## Analytical marts in ClickHouse

| Mart | Description |
|------|-------------|
| mart_product_sales | Top products, revenue by category, avg rating |
| mart_customer_sales | Top customers, country distribution, avg check |
| mart_time_sales | Monthly/yearly trends, avg order by month |
| mart_store_sales | Top stores, city/country distribution, avg check |
| mart_supplier_sales | Top suppliers, avg price, country distribution |
| mart_product_quality | Rating vs sales correlation, most reviewed products |

## Stop

```bash
docker-compose down -v
```
