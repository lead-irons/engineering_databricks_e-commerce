-- =====================================================
-- GOLD: star schema for Power BI semantic modeling
-- Fact table at order-item grain + dimension tables
-- =====================================================

CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_customers
COMMENT 'Customer dimension – one row per customer_id'
CLUSTER BY (customer_id)
AS
SELECT DISTINCT
  c.customer_id,
  c.customer_unique_id,
  c.zip_code_prefix,
  c.city,
  c.state
FROM olist.silver.customers c;


CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_products
COMMENT 'Product dimension – one row per product_id with English category'
CLUSTER BY (product_id)
AS
SELECT
  p.product_id,
  p.product_category,
  p.weight_g,
  p.length_cm,
  p.height_cm,
  p.width_cm,
  p.volume_cm3,
  p.photos_qty
FROM olist.silver.products p;


CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_sellers
COMMENT 'Seller dimension – one row per seller_id'
CLUSTER BY (seller_id)
AS
SELECT DISTINCT
  s.seller_id,
  s.zip_code_prefix,
  s.city,
  s.state
FROM olist.silver.sellers s;


CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_orders
COMMENT 'Order dimension – one row per order_id with delivery metrics and review score'
CLUSTER BY (order_id)
AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  CAST(o.order_purchase_timestamp AS DATE)      AS order_purchase_date,
  CAST(o.order_approved_at AS DATE)             AS order_approved_date,
  CAST(o.order_delivered_carrier_date AS DATE)  AS delivered_carrier_date,
  CAST(o.order_delivered_customer_date AS DATE) AS delivered_customer_date,
  CAST(o.order_estimated_delivery_date AS DATE) AS estimated_delivery_date,
  o.delivery_days,
  o.delivery_delay_days,
  o.is_late,
  r.review_score
FROM olist.silver.orders o
LEFT JOIN olist.silver.order_reviews r ON o.order_id = r.order_id;


CREATE OR REFRESH MATERIALIZED VIEW olist.gold.fact_sales
COMMENT 'Sales fact table – one row per order item (order_id + order_item_id)'
CLUSTER BY (order_id)
AS
SELECT
  oi.order_id,
  oi.order_item_id,
  oi.product_id,
  oi.seller_id,
  o.customer_id,
  oi.shipping_limit_date,
  oi.price,
  oi.freight_value,
  oi.item_total
FROM olist.silver.order_items oi
JOIN olist.silver.orders o ON oi.order_id = o.order_id;


CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_date
COMMENT 'Date dimension covering the full order date range for Power BI time intelligence'
CLUSTER BY (date)
AS
WITH bounds AS (
  SELECT
    MIN(date(order_purchase_timestamp)) AS start_date,
    MAX(date(order_purchase_timestamp)) AS end_date
  FROM olist.silver.orders
),
date_range AS (
  SELECT explode(sequence(start_date, end_date, INTERVAL 1 DAY)) AS date_val
  FROM bounds
)
SELECT
  date_val                                                            AS date,
  YEAR(date_val)                                                      AS year,
  QUARTER(date_val)                                                   AS quarter,
  CONCAT('Q', QUARTER(date_val), ' ', YEAR(date_val))                 AS quarter_year,
  MONTH(date_val)                                                     AS month,
  DATE_FORMAT(date_val, 'MMMM')                                       AS month_name,
  CONCAT(DATE_FORMAT(date_val, 'MMM'), ' ', YEAR(date_val))           AS month_year,
  DAYOFMONTH(date_val)                                                AS day_of_month,
  DAYOFWEEK(date_val)                                                  AS day_of_week,
  DATE_FORMAT(date_val, 'EEEE')                                       AS day_of_week_name,
  DAYOFYEAR(date_val)                                                  AS day_of_year,
  WEEKOFYEAR(date_val)                                                 AS week_of_year,
  CASE WHEN DAYOFWEEK(date_val) IN (1, 7) THEN TRUE ELSE FALSE END   AS is_weekend,
  CASE WHEN DAYOFMONTH(date_val) = 1 THEN TRUE ELSE FALSE END         AS is_month_start,
  CASE WHEN DAYOFMONTH(date_val) = DAY(LAST_DAY(date_val))
       THEN TRUE ELSE FALSE END                                        AS is_month_end
FROM date_range;