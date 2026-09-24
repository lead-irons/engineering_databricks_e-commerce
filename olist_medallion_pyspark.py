"""
Olist E-Commerce Medallion Pipeline — PySpark Edition
=====================================================
PySpark equivalent of the SQL-based Lakeflow Spark Declarative Pipeline:
  - 01_bronze.sql  → Bronze layer (raw CSV ingestion)
  - 02_silver.sql  → Silver layer (cleaned, typed, deduplicated, validated)
  - 03_gold.sql    → Gold layer (star schema for BI)
  - gold_aggregations.sql → All 31 Power BI DAX measures as PySpark

Source: Brazilian E-Commerce Public Dataset by Olist (Kaggle)
        Stored in /Volumes/olist/bronze/raw_files/

Usage: Run in a Databricks notebook or any PySpark environment with
       access to the olist catalog (Unity Catalog).

Author: ddisrael1@alum.up.edu.ph
Date:   September 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, BooleanType, LongType
)

# =====================================================
# Configuration
# =====================================================

spark = SparkSession.builder.appName("OlistMedallionPipeline").getOrCreate()

CATALOG = "olist"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
RAW_PATH = "/Volumes/olist/bronze/raw_files"


def write_table(df, catalog, schema, table_name):
    """Save a DataFrame as a managed Delta table."""
    full_name = f"{catalog}.{schema}.{table_name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    print(f"  ✓ {full_name} ({df.count():,} rows)")
    return full_name


def table(catalog, schema, name):
    """Read a registered table."""
    return spark.table(f"{catalog}.{schema}.{name}")


# =====================================================
# BRONZE LAYER — Raw Ingestion
# Equivalent to 01_bronze.sql (9 CREATE STREAMING TABLE)
# =====================================================
print("\n" + "=" * 60)
print("BRONZE LAYER — Raw CSV Ingestion")
print("=" * 60)

from pyspark.sql.functions import input_file_name, current_timestamp

BRONZE_TABLES = [
    "customers", "orders", "order_items", "order_payments",
    "order_reviews", "products", "sellers", "geolocation",
    "category_translation",
]

# --- customers ---
bronze_customers = (
    spark.read.csv(f"{RAW_PATH}/customers/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_customers, CATALOG, BRONZE_SCHEMA, "customers")

# --- orders ---
bronze_orders = (
    spark.read.csv(f"{RAW_PATH}/orders/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_orders, CATALOG, BRONZE_SCHEMA, "orders")

# --- order_items ---
bronze_order_items = (
    spark.read.csv(f"{RAW_PATH}/order_items/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_order_items, CATALOG, BRONZE_SCHEMA, "order_items")

# --- order_payments ---
bronze_order_payments = (
    spark.read.csv(f"{RAW_PATH}/order_payments/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_order_payments, CATALOG, BRONZE_SCHEMA, "order_payments")

# --- order_reviews (multiLine for free-text comments) ---
bronze_order_reviews = (
    spark.read.csv(f"{RAW_PATH}/order_reviews/", header=True, inferSchema=False,
                   multiLine=True, escape='"')
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_order_reviews, CATALOG, BRONZE_SCHEMA, "order_reviews")

# --- products ---
bronze_products = (
    spark.read.csv(f"{RAW_PATH}/products/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_products, CATALOG, BRONZE_SCHEMA, "products")

# --- sellers ---
bronze_sellers = (
    spark.read.csv(f"{RAW_PATH}/sellers/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_sellers, CATALOG, BRONZE_SCHEMA, "sellers")

# --- geolocation ---
bronze_geolocation = (
    spark.read.csv(f"{RAW_PATH}/geolocation/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_geolocation, CATALOG, BRONZE_SCHEMA, "geolocation")

# --- category_translation ---
bronze_category_translation = (
    spark.read.csv(f"{RAW_PATH}/category_translation/", header=True, inferSchema=False)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
write_table(bronze_category_translation, CATALOG, BRONZE_SCHEMA, "category_translation")

print(f"\n✅ Bronze complete — 9 tables loaded")


# =====================================================
# SILVER LAYER — Cleaned, Typed, Deduplicated, Validated
# Equivalent to 02_silver.sql (8 CREATE MATERIALIZED VIEW)
# =====================================================
print("\n" + "=" * 60)
print("SILVER LAYER — Cleaned & Validated")
print("=" * 60)

# Read bronze tables
b_customers = table(CATALOG, BRONZE_SCHEMA, "customers")
b_orders = table(CATALOG, BRONZE_SCHEMA, "orders")
b_order_items = table(CATALOG, BRONZE_SCHEMA, "order_items")
b_order_payments = table(CATALOG, BRONZE_SCHEMA, "order_payments")
b_order_reviews = table(CATALOG, BRONZE_SCHEMA, "order_reviews")
b_products = table(CATALOG, BRONZE_SCHEMA, "products")
b_sellers = table(CATALOG, BRONZE_SCHEMA, "sellers")
b_geolocation = table(CATALOG, BRONZE_SCHEMA, "geolocation")
b_category_translation = table(CATALOG, BRONZE_SCHEMA, "category_translation")

# --- 1. silver.customers ---
# Constraint: customer_id NOT NULL, customer_unique_id NOT NULL, state length = 2
silver_customers = (
    b_customers
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.lpad(F.trim("customer_zip_code_prefix"), 5, "0").alias("zip_code_prefix"),
        F.initcap(F.trim("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"),
    )
    .filter(F.col("customer_id").isNotNull() & F.col("customer_unique_id").isNotNull())
    .filter(F.length("state") == 2)
    .dropDuplicates(["customer_id"])
)
write_table(silver_customers, CATALOG, SILVER_SCHEMA, "customers")

# --- 2. silver.orders ---
# Deduplicate by order_id (keep latest _ingested_at), derive delivery metrics
orders_w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

silver_orders = (
    b_orders
    .select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.col("order_status"),
        F.to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
        F.to_timestamp("order_approved_at").alias("order_approved_at"),
        F.to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
        F.to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_date"),
        F.to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
        F.col("_ingested_at"),
    )
    .withColumn("rn", F.row_number().over(orders_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("order_status", F.lower(F.trim("order_status")))
    .withColumn("delivery_days",
                F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("delivery_delay_days",
                F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
    .withColumn("is_late",
                F.when(F.col("order_delivered_customer_date").isNull(), F.lit(None))
                .when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), F.lit(True))
                .otherwise(F.lit(False)))
    .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    .filter(F.col("order_purchase_timestamp").isNotNull())
    .filter(~((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull()))
    .select(
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date", "delivery_days", "delivery_delay_days", "is_late",
    )
)
write_table(silver_orders, CATALOG, SILVER_SCHEMA, "orders")

# --- 3. silver.order_items ---
# Deduplicate by order_id + order_item_id, compute item_total
items_w = Window.partitionBy("order_id", "order_item_id").orderBy(F.col("_ingested_at").desc())

silver_order_items = (
    b_order_items
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("order_item_id").cast(IntegerType()).alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        F.col("price").cast("decimal(12,2)"),
        F.col("freight_value").cast("decimal(12,2)"),
        F.col("_ingested_at"),
    )
    .withColumn("rn", F.row_number().over(items_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("item_total", F.col("price") + F.col("freight_value"))
    .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull() & F.col("seller_id").isNotNull())
    .filter(F.col("price") >= 0)
    .filter(F.col("freight_value") >= 0)
    .select("order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value", "item_total")
)
write_table(silver_order_items, CATALOG, SILVER_SCHEMA, "order_items")

# --- 4. silver.order_payments ---
pay_w = Window.partitionBy("order_id", "payment_sequential").orderBy(F.col("_ingested_at").desc())

silver_order_payments = (
    b_order_payments
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("payment_sequential").cast(IntegerType()).alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        F.col("payment_installments").cast(IntegerType()).alias("payment_installments"),
        F.col("payment_value").cast("decimal(12,2)"),
        F.col("_ingested_at"),
    )
    .withColumn("rn", F.row_number().over(pay_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .filter(F.col("order_id").isNotNull() & (F.col("payment_value") >= 0))
    .filter(F.col("payment_type").isin("credit_card", "boleto", "voucher", "debit_card"))
    .select("order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value")
)
write_table(silver_order_payments, CATALOG, SILVER_SCHEMA, "order_payments")

# --- 5. silver.order_reviews ---
# Keep latest review per order_id
rev_w = Window.partitionBy("order_id").orderBy(
    F.try_cast(F.col("review_answer_timestamp"), TimestampType()).desc())

silver_order_reviews = (
    b_order_reviews
    .select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        F.try_cast("review_score", IntegerType()).alias("review_score"),
        F.try_cast("review_creation_date", TimestampType()).alias("review_creation_date"),
        F.try_cast("review_answer_timestamp", TimestampType()).alias("review_answer_timestamp"),
    )
    .withColumn("rn", F.row_number().over(rev_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .filter(F.col("review_id").isNotNull() & F.col("order_id").isNotNull())
    .filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5))
    .select("review_id", "order_id", "review_score",
            "review_creation_date", "review_answer_timestamp")
)
write_table(silver_order_reviews, CATALOG, SILVER_SCHEMA, "order_reviews")

# --- 6. silver.products ---
# Join with category translation for English names, compute volume
products_dedup = b_products.dropDuplicates([
    c for c in b_products.columns if c not in ("_ingested_at", "_source_file")
])

cat_trans = b_category_translation.select(
    F.trim("product_category_name").alias("product_category_name"),
    F.trim("product_category_name_english").alias("product_category_name_english"),
).dropDuplicates(["product_category_name"])

silver_products = (
    products_dedup.alias("p")
    .join(cat_trans.alias("t"),
          F.trim(F.col("p.product_category_name")) == F.col("t.product_category_name"),
          "left")
    .select(
        F.trim("p.product_id").alias("product_id"),
        F.coalesce(F.col("t.product_category_name_english"),
                   F.trim("p.product_category_name"),
                   F.lit("unknown")).alias("product_category"),
        F.col("p.product_weight_g").cast(DoubleType()).alias("weight_g"),
        F.col("p.product_length_cm").cast(DoubleType()).alias("length_cm"),
        F.col("p.product_height_cm").cast(DoubleType()).alias("height_cm"),
        F.col("p.product_width_cm").cast(DoubleType()).alias("width_cm"),
        F.col("p.product_photos_qty").cast(IntegerType()).alias("photos_qty"),
        (F.col("p.product_length_cm").cast(DoubleType())
         * F.col("p.product_height_cm").cast(DoubleType())
         * F.col("p.product_width_cm").cast(DoubleType())).alias("volume_cm3"),
    )
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("product_category") != "unknown")
)
write_table(silver_products, CATALOG, SILVER_SCHEMA, "products")

# --- 7. silver.sellers ---
silver_sellers = (
    b_sellers
    .select(
        F.trim("seller_id").alias("seller_id"),
        F.lpad(F.trim("seller_zip_code_prefix"), 5, "0").alias("zip_code_prefix"),
        F.initcap(F.trim("seller_city")).alias("city"),
        F.upper(F.trim("seller_state")).alias("state"),
    )
    .filter(F.col("seller_id").isNotNull())
    .filter(F.length("state") == 2)
    .dropDuplicates(["seller_id"])
)
write_table(silver_sellers, CATALOG, SILVER_SCHEMA, "sellers")

# --- 8. silver.geolocation ---
silver_geolocation = (
    b_geolocation
    .groupBy(F.lpad(F.trim("geolocation_zip_code_prefix"), 5, "0").alias("zip_code_prefix"))
    .agg(
        F.avg(F.col("geolocation_lat").cast(DoubleType())).alias("latitude"),
        F.avg(F.col("geolocation_lng").cast(DoubleType())).alias("longitude"),
        F.first(F.initcap(F.trim("geolocation_city"))).alias("city"),
        F.first(F.upper(F.trim("geolocation_state"))).alias("state"),
    )
)
write_table(silver_geolocation, CATALOG, SILVER_SCHEMA, "geolocation")

print(f"\n✅ Silver complete — 8 tables cleaned & validated")


# =====================================================
# GOLD LAYER — Star Schema
# Equivalent to 03_gold.sql (6 CREATE MATERIALIZED VIEW)
# =====================================================
print("\n" + "=" * 60)
print("GOLD LAYER — Star Schema for BI")
print("=" * 60)

# Read silver tables
s_customers = table(CATALOG, SILVER_SCHEMA, "customers")
s_orders = table(CATALOG, SILVER_SCHEMA, "orders")
s_order_items = table(CATALOG, SILVER_SCHEMA, "order_items")
s_order_reviews = table(CATALOG, SILVER_SCHEMA, "order_reviews")
s_products = table(CATALOG, SILVER_SCHEMA, "products")
s_sellers = table(CATALOG, SILVER_SCHEMA, "sellers")

# --- 1. dim_customers ---
gold_dim_customers = (
    s_customers
    .select("customer_id", "customer_unique_id", "zip_code_prefix", "city", "state")
    .dropDuplicates(["customer_id"])
)
write_table(gold_dim_customers, CATALOG, GOLD_SCHEMA, "dim_customers")

# --- 2. dim_products ---
gold_dim_products = s_products.select(
    "product_id", "product_category", "weight_g", "length_cm",
    "height_cm", "width_cm", "volume_cm3", "photos_qty"
)
write_table(gold_dim_products, CATALOG, GOLD_SCHEMA, "dim_products")

# --- 3. dim_sellers ---
gold_dim_sellers = (
    s_sellers
    .select("seller_id", "zip_code_prefix", "city", "state")
    .dropDuplicates(["seller_id"])
)
write_table(gold_dim_sellers, CATALOG, GOLD_SCHEMA, "dim_sellers")

# --- 4. dim_orders ---
# Join orders with reviews, add 5 DATE columns
gold_dim_orders = (
    s_orders.alias("o")
    .join(s_order_reviews.alias("r"), F.col("o.order_id") == F.col("r.order_id"), "left")
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.order_status"),
        F.col("o.order_purchase_timestamp"),
        F.col("o.order_approved_at"),
        F.col("o.order_delivered_carrier_date"),
        F.col("o.order_delivered_customer_date"),
        F.col("o.order_estimated_delivery_date"),
        F.col("o.order_purchase_timestamp").cast(DateType()).alias("order_purchase_date"),
        F.col("o.order_approved_at").cast(DateType()).alias("order_approved_date"),
        F.col("o.order_delivered_carrier_date").cast(DateType()).alias("delivered_carrier_date"),
        F.col("o.order_delivered_customer_date").cast(DateType()).alias("delivered_customer_date"),
        F.col("o.order_estimated_delivery_date").cast(DateType()).alias("estimated_delivery_date"),
        F.col("o.delivery_days"),
        F.col("o.delivery_delay_days"),
        F.col("o.is_late"),
        F.col("r.review_score"),
    )
)
write_table(gold_dim_orders, CATALOG, GOLD_SCHEMA, "dim_orders")

# --- 5. fact_sales ---
# One row per order item (order_id + order_item_id)
gold_fact_sales = (
    s_order_items.alias("oi")
    .join(s_orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"))
    .select(
        F.col("oi.order_id"),
        F.col("oi.order_item_id"),
        F.col("oi.product_id"),
        F.col("oi.seller_id"),
        F.col("o.customer_id"),
        F.col("oi.shipping_limit_date"),
        F.col("oi.price"),
        F.col("oi.freight_value"),
        F.col("oi.item_total"),
    )
)
write_table(gold_fact_sales, CATALOG, GOLD_SCHEMA, "fact_sales")

# --- 6. dim_date ---
# Generate date range from min to max order purchase date
bounds = s_orders.agg(
    F.min(F.col("order_purchase_timestamp").cast(DateType())).alias("start_date"),
    F.max(F.col("order_purchase_timestamp").cast(DateType())).alias("end_date")
).collect()
start_date, end_date = bounds[0]["start_date"], bounds[0]["end_date"]

gold_dim_date = (
    spark.range(0, (end_date.toordinal() - start_date.toordinal()) + 1)
    .select((F.expr(f"date('{start_date}') + interval {0} days") + F.col("id")).alias("date"))
    .select(
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.concat(F.lit("Q"), F.quarter("date"), F.lit(" "), F.year("date")).alias("quarter_year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.concat(F.date_format("date", "MMM"), F.lit(" "), F.year("date")).alias("month_year"),
        F.dayofmonth("date").alias("day_of_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_of_week_name"),
        F.dayofyear("date").alias("day_of_year"),
        F.weekofyear("date").alias("week_of_year"),
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        F.when(F.dayofmonth("date") == 1, True).otherwise(False).alias("is_month_start"),
        F.when(F.dayofmonth("date") == F.day(F.last_day("date")), True).otherwise(False).alias("is_month_end"),
    )
)
write_table(gold_dim_date, CATALOG, GOLD_SCHEMA, "dim_date")

print(f"\n✅ Gold complete — 6 star schema tables created")


# =====================================================
# GOLD AGGREGATIONS — 31 Power BI DAX Measure Equivalents
# Equivalent to gold_aggregations.sql (6 sections)
# =====================================================
print("\n" + "=" * 60)
print("GOLD AGGREGATIONS — Power BI Measure Equivalents")
print("=" * 60)

# Register gold tables as temp views for spark.sql()
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")
for t in ["dim_customers", "dim_products", "dim_sellers", "dim_orders", "fact_sales", "dim_date"]:
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{t}").createOrReplaceTempView(t)

# ───────────────────────────────────────────────────
# SECTION 1: SALES MEASURES (DAX Display Folder: "Sales")
# ───────────────────────────────────────────────────
print("\n--- Section 1: Sales Measures ---")

fact_sales = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")

sales_measures = fact_sales.agg(
    F.sum("item_total").alias("total_revenue"),
    F.sum("price").alias("total_product_revenue"),
    F.sum("freight_value").alias("total_freight"),
    F.count("*").alias("total_order_items"),
    F.countDistinct("order_id").alias("total_orders"),
)
# Derived: avg_order_value, avg_items_per_order, avg_freight_per_order, freight_ratio
sales_measures = sales_measures.withColumn(
    "avg_order_value",
    F.round(F.col("total_revenue") / F.col("total_orders"), 2)
).withColumn(
    "avg_items_per_order",
    F.round(F.col("total_order_items") / F.col("total_orders"), 2)
).withColumn(
    "avg_freight_per_order",
    F.round(F.col("total_freight") / F.col("total_orders"), 2)
).withColumn(
    "freight_ratio",
    F.round(F.col("total_freight") / F.when(F.col("total_revenue") == 0, F.lit(None)).otherwise(F.col("total_revenue")), 4)
)
sales_measures.show(vertical=True)

# ───────────────────────────────────────────────────
# SECTION 2: DELIVERY MEASURES (DAX Display Folder: "Delivery")
# ───────────────────────────────────────────────────
print("\n--- Section 2: Delivery Measures ---")

dim_orders = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_orders")

delivery_measures = (
    dim_orders.filter(F.col("is_late").isNotNull())
    .agg(
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_deliveries"),
        F.count("*").alias("total_orders"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg("delivery_delay_days"), 2).alias("avg_delivery_delay"),
    )
    .withColumn(
        "on_time_delivery_rate_pct",
        F.round((F.col("total_orders") - F.col("late_deliveries"))
                / F.when(F.col("total_orders") == 0, F.lit(None)).otherwise(F.col("total_orders")) * 100, 1)
    )
)
delivery_measures.show(vertical=True)

# ───────────────────────────────────────────────────
# SECTION 3: PRODUCT MEASURES (DAX Display Folder: "Products")
# ───────────────────────────────────────────────────
print("\n--- Section 3: Product Measures ---")

dim_products = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_products")

product_measures = (
    fact_sales.join(dim_products, "product_id")
    .agg(
        F.countDistinct("product_id").alias("total_products_sold"),
        F.round(F.avg("weight_g"), 2).alias("avg_product_weight_g"),
    )
)
product_measures.show(vertical=True)

# ───────────────────────────────────────────────────
# SECTION 4: TIME INTELLIGENCE (DAX Display Folder: "Time Intelligence")
# Uses active date relationship: dim_date.date → dim_orders.order_purchase_date
# ───────────────────────────────────────────────────
print("\n--- Section 4: Time Intelligence ---")

dim_date = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_date")

# Monthly revenue and orders with SPLY, MoM, YoY%, MoM%
monthly_ti = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.order_purchase_date") == F.col("dim_date.date"))
    .groupBy(
        F.col("dim_date.year"),
        F.col("dim_date.month"),
        F.col("dim_date.month_year"),
        F.date_trunc("month", F.col("dim_orders.order_purchase_timestamp")).alias("month_start"),
    )
    .agg(
        F.sum("item_total").alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
    )
)

# YTD
ytd_ti = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.order_purchase_date") == F.col("dim_date.date"))
    .groupBy(F.col("dim_date.year"))
    .agg(
        F.sum("item_total").alias("revenue_ytd"),
        F.countDistinct("order_id").alias("orders_ytd"),
    )
)

# Window for LAG (SPLY = 12 months back, prev month = 1 back)
monthly_w = Window.orderBy("month_start")

time_intelligence = (
    monthly_ti.join(ytd_ti, "year")
    .withColumn("revenue_sply", F.lag("revenue", 12).over(monthly_w))
    .withColumn("revenue_prev_month", F.lag("revenue", 1).over(monthly_w))
    .withColumn("revenue_mom_pct",
                F.round((F.col("revenue") - F.lag("revenue", 1).over(monthly_w))
                        / F.when(F.lag("revenue", 1).over(monthly_w) == 0, F.lit(None))
                          .otherwise(F.lag("revenue", 1).over(monthly_w)) * 100, 1))
    .withColumn("revenue_yoy_pct",
                F.round((F.col("revenue") - F.lag("revenue", 12).over(monthly_w))
                        / F.when(F.lag("revenue", 12).over(monthly_w) == 0, F.lit(None))
                          .otherwise(F.lag("revenue", 12).over(monthly_w)) * 100, 1))
    .select("month_year", "revenue", "orders", "revenue_ytd", "orders_ytd",
            "revenue_sply", "revenue_prev_month", "revenue_mom_pct", "revenue_yoy_pct")
    .orderBy("month_start")
)
time_intelligence.show(truncate=False)

# ───────────────────────────────────────────────────
# SECTION 5: MULTI-DATE MEASURES (DAX Display Folder: "Multi-Date")
# Uses USERELATIONSHIP to activate inactive date relationships
# ───────────────────────────────────────────────────
print("\n--- Section 5: Multi-Date Measures ---")

# Revenue by Approved Date
rev_by_approved = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.order_approved_date") == F.col("dim_date.date"))
    .agg(F.sum("item_total").alias("revenue_by_approved_date"))
)
rev_by_approved.show()

# Revenue by Carrier Pickup Date
rev_by_carrier = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.delivered_carrier_date") == F.col("dim_date.date"))
    .agg(F.sum("item_total").alias("revenue_by_carrier_pickup_date"))
)
rev_by_carrier.show()

# Revenue by Delivery Date
rev_by_delivery = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.delivered_customer_date") == F.col("dim_date.date"))
    .agg(F.sum("item_total").alias("revenue_by_delivery_date"))
)
rev_by_delivery.show()

# Revenue by Estimated Delivery Date
rev_by_estimated = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.estimated_delivery_date") == F.col("dim_date.date"))
    .agg(F.sum("item_total").alias("revenue_by_estimated_delivery_date"))
)
rev_by_estimated.show()

# Orders by Delivery Date
orders_by_delivery = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.delivered_customer_date") == F.col("dim_date.date"))
    .agg(F.countDistinct("order_id").alias("orders_by_delivery_date"))
)
orders_by_delivery.show()

# Orders by Approved Date
orders_by_approved = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.order_approved_date") == F.col("dim_date.date"))
    .agg(F.countDistinct("order_id").alias("orders_by_approved_date"))
)
orders_by_approved.show()

# Revenue YTD by Delivery Date
rev_ytd_by_delivery = (
    fact_sales.join(dim_orders, "order_id")
    .join(dim_date, F.col("dim_orders.delivered_customer_date") == F.col("dim_date.date"))
    .groupBy(F.col("dim_date.year"))
    .agg(F.sum("item_total").alias("revenue_ytd_by_delivery_date"))
)
rev_ytd_by_delivery.show()

# Revenue YoY% by Delivery Date
monthly_delivery = (
    fact_sales.join(dim_orders, "order_id")
    .filter(F.col("dim_orders.delivered_customer_date").isNotNull())
    .groupBy(F.date_trunc("month", F.col("dim_orders.delivered_customer_date")).alias("month_start"))
    .agg(F.sum("item_total").alias("revenue"))
)

delivery_w = Window.orderBy("month_start")
rev_yoy_by_delivery = (
    monthly_delivery
    .withColumn("revenue_sply", F.lag("revenue", 12).over(delivery_w))
    .withColumn("revenue_yoy_pct_by_delivery_date",
                F.round((F.col("revenue") - F.lag("revenue", 12).over(delivery_w))
                        / F.when(F.lag("revenue", 12).over(delivery_w) == 0, F.lit(None))
                          .otherwise(F.lag("revenue", 12).over(delivery_w)) * 100, 1))
    .orderBy("month_start")
)
rev_yoy_by_delivery.show(truncate=False)

# Avg Delivery Days by Delivery Date
avg_delivery_by_date = (
    dim_orders.join(dim_date, F.col("dim_orders.delivered_customer_date") == F.col("dim_date.date"))
    .filter(F.col("delivery_days").isNotNull())
    .agg(F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days_by_delivery_date"))
)
avg_delivery_by_date.show()

# On-Time Rate by Delivery Date
on_time_by_delivery = (
    dim_orders.join(dim_date, F.col("dim_orders.delivered_customer_date") == F.col("dim_date.date"))
    .filter(F.col("is_late").isNotNull())
    .agg(
        F.round((F.count("*") - F.sum(F.when(F.col("is_late"), 1).otherwise(0)))
                / F.when(F.count("*") == 0, F.lit(None)).otherwise(F.count("*")) * 100, 1).alias("on_time_rate_by_delivery_date")
    )
)
on_time_by_delivery.show()

# ───────────────────────────────────────────────────
# SECTION 6: ORIGINAL EXPLORATORY QUERIES (Silver Layer)
# Reference queries kept for validation
# ───────────────────────────────────────────────────
print("\n--- Section 6: Exploratory Queries (Silver) ---")

# Register silver tables
for t in ["customers", "orders", "order_items", "order_payments", "order_reviews", "products", "sellers"]:
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{t}").createOrReplaceTempView(f"silver_{t}")

# Daily revenue summary
daily_revenue = spark.sql("""
    SELECT
      date(o.order_purchase_timestamp) AS order_date,
      COUNT(DISTINCT o.order_id)       AS total_orders,
      COUNT(*)                         AS total_items,
      SUM(oi.price)                    AS total_revenue,
      SUM(oi.freight_value)            AS total_freight,
      SUM(oi.item_total)               AS total_order_value,
      ROUND(SUM(oi.item_total) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value,
      SUM(CASE WHEN o.is_late THEN 1 ELSE 0 END) AS late_orders
    FROM silver_orders o
    JOIN silver_order_items oi ON o.order_id = oi.order_id
    GROUP BY date(o.order_purchase_timestamp)
    ORDER BY order_date
""")
daily_revenue.show(truncate=False)

# Product category performance
category_performance = spark.sql("""
    SELECT
      p.product_category,
      COUNT(DISTINCT oi.order_id)  AS total_orders,
      COUNT(*)                     AS total_items_sold,
      SUM(oi.price)                AS total_revenue,
      ROUND(AVG(oi.price), 2)      AS avg_item_price,
      ROUND(AVG(r.review_score), 2) AS avg_review_score
    FROM silver_order_items oi
    JOIN silver_products p     ON oi.product_id = p.product_id
    LEFT JOIN silver_order_reviews r ON oi.order_id = r.order_id
    GROUP BY p.product_category
    ORDER BY total_revenue DESC
""")
category_performance.show(truncate=False)

# Seller performance
seller_performance = spark.sql("""
    SELECT
      s.seller_id,
      s.city                       AS seller_city,
      s.state                      AS seller_state,
      COUNT(DISTINCT oi.order_id)  AS total_orders,
      COUNT(*)                     AS total_items_sold,
      SUM(oi.price)                AS total_revenue,
      SUM(oi.freight_value)        AS total_freight,
      ROUND(AVG(r.review_score), 2) AS avg_review_score
    FROM silver_order_items oi
    JOIN silver_sellers s      ON oi.seller_id = s.seller_id
    LEFT JOIN silver_order_reviews r ON oi.order_id = r.order_id
    GROUP BY s.seller_id, s.city, s.state
    ORDER BY total_revenue DESC
""")
seller_performance.show(truncate=False)

# Payment type distribution
payment_distribution = spark.sql("""
    SELECT
      payment_type,
      COUNT(DISTINCT order_id)     AS total_orders,
      SUM(payment_value)           AS total_payment_value,
      ROUND(AVG(payment_value), 2) AS avg_payment_value,
      ROUND(AVG(payment_installments), 1) AS avg_installments
    FROM silver_order_payments
    GROUP BY payment_type
    ORDER BY total_payment_value DESC
""")
payment_distribution.show(truncate=False)

# Customer order history and spend summary
customer_summary = spark.sql("""
    SELECT
      c.customer_id,
      c.customer_unique_id,
      c.city,
      c.state,
      COUNT(DISTINCT o.order_id)     AS total_orders,
      SUM(oi.item_total)            AS total_spend,
      ROUND(SUM(oi.item_total) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value,
      MAX(o.order_purchase_timestamp) AS last_order_date,
      MIN(o.order_purchase_timestamp) AS first_order_date,
      ROUND(AVG(r.review_score), 2) AS avg_review_score
    FROM silver_customers c
    JOIN silver_orders o        ON c.customer_id = o.customer_id
    JOIN silver_order_items oi  ON o.order_id = oi.order_id
    LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
    GROUP BY c.customer_id, c.customer_unique_id, c.city, c.state
    ORDER BY total_spend DESC
""")
customer_summary.show(truncate=False)

print("\n" + "=" * 60)
print("✅ Pipeline complete — all 3 medallion layers + 31 measures")
print("=" * 60)