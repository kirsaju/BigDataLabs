from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count as _count, avg as _avg, concat_ws
)
import clickhouse_connect

spark = SparkSession.builder \
    .appName("ETL: Star Schema -> ClickHouse Marts") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

PG_URL = "jdbc:postgresql://postgres:5432/bigdata_lab2"
PG_PROPS = {
    "user": "my_user",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

# ClickHouse native client
ch_client = clickhouse_connect.get_client(host="clickhouse", port=8123, username="click", password="click")


def write_to_clickhouse(df, table_name):
    """Convert Spark DataFrame to pandas and write to ClickHouse via native client."""
    pdf = df.toPandas()

    # Drop table if exists and create with MergeTree engine
    ch_client.command(f"DROP TABLE IF EXISTS {table_name}")

    # Build CREATE TABLE statement from pandas dtypes
    col_defs = []
    for c in pdf.columns:
        dtype = str(pdf[c].dtype)
        if "int" in dtype:
            ch_type = "Nullable(Int64)"
        elif "float" in dtype:
            ch_type = "Nullable(Float64)"
        else:
            ch_type = "Nullable(String)"
        col_defs.append(f"`{c}` {ch_type}")

    ddl = f"CREATE TABLE {table_name} ({', '.join(col_defs)}) ENGINE = MergeTree() ORDER BY tuple()"
    ch_client.command(ddl)

    # Insert data
    ch_client.insert_df(table_name, pdf)
    print(f"  {table_name}: {len(pdf)} rows written to ClickHouse.")


# -------------------------------------------------------------------
# 1. Read star schema tables from PostgreSQL
# -------------------------------------------------------------------
print("Reading star schema from PostgreSQL...")

fact_sales = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
dim_customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
dim_product = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
dim_store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
dim_supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
dim_date = spark.read.jdbc(PG_URL, "dim_date", properties=PG_PROPS)

print(f"fact_sales: {fact_sales.count()} rows")

# -------------------------------------------------------------------
# 2. Build joined base datasets
# -------------------------------------------------------------------

fact_product = fact_sales.join(dim_product, on="product_id", how="left")
fact_customer = fact_sales.join(dim_customer, on="customer_id", how="left")
fact_date = fact_sales.join(dim_date, on="date_id", how="left")
fact_store = fact_sales.join(dim_store, on="store_id", how="left")
fact_supplier = fact_sales.join(dim_supplier, on="supplier_id", how="left") \
    .join(dim_product, on="product_id", how="left")

# -------------------------------------------------------------------
# 3. Build 6 mart DataFrames
# -------------------------------------------------------------------

# --- mart_product_sales ---
print("Building mart_product_sales...")
mart_product_sales = fact_product.groupBy("product_name", "product_category").agg(
    _sum("sale_quantity").alias("total_quantity_sold"),
    _sum("sale_total_price").alias("total_revenue"),
    _avg("product_rating").alias("avg_rating"),
    _sum("product_reviews").alias("total_reviews")
).orderBy(col("total_revenue").desc())
print(f"  rows: {mart_product_sales.count()}")

# --- mart_customer_sales ---
print("Building mart_customer_sales...")
mart_customer_sales = fact_customer.withColumn(
    "customer_name",
    concat_ws(" ", col("customer_first_name"), col("customer_last_name"))
).groupBy("customer_name", "customer_country").agg(
    _sum("sale_total_price").alias("total_spent"),
    _count("*").alias("num_purchases"),
    _avg("sale_total_price").alias("avg_check")
)
print(f"  rows: {mart_customer_sales.count()}")

# --- mart_time_sales ---
print("Building mart_time_sales...")
mart_time_sales = fact_date.groupBy("year", "month").agg(
    _sum("sale_total_price").alias("total_sales"),
    _count("*").alias("num_orders"),
    _avg("sale_total_price").alias("avg_order_size")
).orderBy("year", "month")
print(f"  rows: {mart_time_sales.count()}")

# --- mart_store_sales ---
print("Building mart_store_sales...")
mart_store_sales = fact_store.groupBy("store_name", "store_city", "store_country").agg(
    _sum("sale_total_price").alias("total_revenue"),
    _count("*").alias("num_sales"),
    _avg("sale_total_price").alias("avg_check")
)
print(f"  rows: {mart_store_sales.count()}")

# --- mart_supplier_sales ---
print("Building mart_supplier_sales...")
mart_supplier_sales = fact_supplier.groupBy("supplier_name", "supplier_country").agg(
    _sum("sale_total_price").alias("total_revenue"),
    _avg("product_price").alias("avg_product_price"),
    _count("*").alias("num_products_sold")
)
print(f"  rows: {mart_supplier_sales.count()}")

# --- mart_product_quality ---
print("Building mart_product_quality...")
mart_product_quality = fact_product.groupBy("product_name", "product_category").agg(
    _avg("product_rating").alias("avg_rating"),
    _sum("product_reviews").alias("total_reviews"),
    _sum("sale_quantity").alias("total_quantity_sold"),
    _sum("sale_total_price").alias("total_revenue")
)
print(f"  rows: {mart_product_quality.count()}")

# -------------------------------------------------------------------
# 4. Write all marts to ClickHouse
# -------------------------------------------------------------------
print("Writing marts to ClickHouse...")

write_to_clickhouse(mart_product_sales, "mart_product_sales")
write_to_clickhouse(mart_customer_sales, "mart_customer_sales")
write_to_clickhouse(mart_time_sales, "mart_time_sales")
write_to_clickhouse(mart_store_sales, "mart_store_sales")
write_to_clickhouse(mart_supplier_sales, "mart_supplier_sales")
write_to_clickhouse(mart_product_quality, "mart_product_quality")

print("All 6 marts written to ClickHouse. ETL complete!")
spark.stop()
