-- =====================================================
-- EXPLORATORY: Gold layer aggregation queries
-- SQL equivalents of all 31 Power BI DAX measures
-- for validating that Power BI displays correct figures.
-- Uses olist.gold star schema (fact_sales + dim tables).
-- =====================================================

-- =====================================================
-- SECTION 1: SALES MEASURES (DAX Display Folder: "Sales")
-- =====================================================

-- Total Revenue = SUM(fact_sales[item_total])
-- Total Product Revenue = SUM(fact_sales[price])
-- Total Freight = SUM(fact_sales[freight_value])
-- Total Order Items = COUNTROWS(fact_sales)
-- Total Orders = DISTINCTCOUNT(fact_sales[order_id])
SELECT
  SUM(fs.item_total)                                     AS total_revenue,
  SUM(fs.price)                                          AS total_product_revenue,
  SUM(fs.freight_value)                                  AS total_freight,
  COUNT(*)                                                AS total_order_items,
  COUNT(DISTINCT fs.order_id)                             AS total_orders,
  -- Derived measures:
  -- Average Order Value = DIVIDE([Total Revenue], [Total Orders], 0)
  ROUND(SUM(fs.item_total) / COUNT(DISTINCT fs.order_id), 2)   AS avg_order_value,
  -- Avg Items per Order = DIVIDE([Total Order Items], [Total Orders], 0)
  ROUND(COUNT(*) / COUNT(DISTINCT fs.order_id), 2)             AS avg_items_per_order,
  -- Avg Freight per Order = DIVIDE([Total Freight], [Total Orders], 0)
  ROUND(SUM(fs.freight_value) / COUNT(DISTINCT fs.order_id), 2) AS avg_freight_per_order,
  -- Freight Ratio = DIVIDE([Total Freight], [Total Revenue], 0)
  ROUND(SUM(fs.freight_value) / NULLIF(SUM(fs.item_total), 0), 4) AS freight_ratio
FROM olist.gold.fact_sales fs;

-- =====================================================
-- SECTION 2: DELIVERY MEASURES (DAX Display Folder: "Delivery")
-- =====================================================

-- Avg Review Score = AVERAGE(dim_orders[review_score])
-- Late Deliveries = CALCULATE(COUNTROWS(dim_orders), dim_orders[is_late] = TRUE())
-- On-Time Delivery Rate = DIVIDE([Total Orders] - [Late Deliveries], [Total Orders], 0)
-- Avg Delivery Days = AVERAGE(dim_orders[delivery_days])
-- Avg Delivery Delay = AVERAGE(dim_orders[delivery_delay_days])
SELECT
  ROUND(AVG(do.review_score), 2)                          AS avg_review_score,
  SUM(CASE WHEN do.is_late THEN 1 ELSE 0 END)             AS late_deliveries,
  COUNT(*)                                                AS total_orders,
  ROUND(
    (COUNT(*) - SUM(CASE WHEN do.is_late THEN 1 ELSE 0 END))
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                                       AS on_time_delivery_rate_pct,
  ROUND(AVG(do.delivery_days), 2)                         AS avg_delivery_days,
  ROUND(AVG(do.delivery_delay_days), 2)                   AS avg_delivery_delay
FROM olist.gold.dim_orders do
WHERE do.is_late IS NOT NULL;

-- =====================================================
-- SECTION 3: PRODUCT MEASURES (DAX Display Folder: "Products")
-- =====================================================

-- Total Products Sold = DISTINCTCOUNT(fact_sales[product_id])
-- Avg Product Weight (g) = AVERAGE(dim_products[weight_g])
SELECT
  COUNT(DISTINCT fs.product_id)     AS total_products_sold,
  ROUND(AVG(dp.weight_g), 2)        AS avg_product_weight_g
FROM olist.gold.fact_sales fs
JOIN olist.gold.dim_products dp ON fs.product_id = dp.product_id;

-- =====================================================
-- SECTION 4: TIME INTELLIGENCE (DAX Display Folder: "Time Intelligence")
-- Uses active date relationship: dim_date.date -> dim_orders.order_purchase_date
-- =====================================================

-- Revenue YTD = TOTALYTD([Total Revenue], dim_date[date])
-- Revenue Prev Month = CALCULATE([Total Revenue], DATEADD(dim_date[date], -1, MONTH))
-- Revenue MoM % = DIVIDE([Total Revenue] - [Revenue Prev Month], [Revenue Prev Month], 0)
-- Revenue Prev Year (SPLY) = CALCULATE([Total Revenue], DATEADD(dim_date[date], -1, YEAR))
-- Revenue YoY % = DIVIDE([Total Revenue] - [Revenue Prev Year], [Revenue Prev Year], 0)
-- Orders YTD = TOTALYTD([Total Orders], dim_date[date])
WITH monthly AS (
  SELECT
    dd.year,
    dd.month,
    dd.month_year,
    DATE_TRUNC('month', do.order_purchase_timestamp) AS month_start,
    SUM(fs.item_total)  AS revenue,
    COUNT(DISTINCT fs.order_id) AS orders
  FROM olist.gold.fact_sales fs
    JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
    JOIN olist.gold.dim_date dd ON do.order_purchase_date = dd.date
  GROUP BY dd.year, dd.month, dd.month_year,
           DATE_TRUNC('month', do.order_purchase_timestamp)
),
ytd AS (
  SELECT
    dd.year,
    SUM(fs.item_total)  AS revenue_ytd,
    COUNT(DISTINCT fs.order_id) AS orders_ytd
  FROM olist.gold.fact_sales fs
    JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
    JOIN olist.gold.dim_date dd ON do.order_purchase_date = dd.date
  GROUP BY dd.year
)
SELECT
  m.month_year,
  m.revenue                                                AS revenue,
  m.orders                                                 AS orders,
  y.revenue_ytd                                            AS revenue_ytd,
  y.orders_ytd                                             AS orders_ytd,
  -- SPLY (Same Period Last Year) = Revenue Prev Year
  LAG(m.revenue, 12) OVER (ORDER BY m.month_start)         AS revenue_sply,
  -- vs Previous Month = Revenue Prev Month
  LAG(m.revenue, 1) OVER (ORDER BY m.month_start)          AS revenue_prev_month,
  -- Revenue MoM %
  ROUND(
    (m.revenue - LAG(m.revenue, 1) OVER (ORDER BY m.month_start))
    / NULLIF(LAG(m.revenue, 1) OVER (ORDER BY m.month_start), 0) * 100, 1
  )                                                         AS revenue_mom_pct,
  -- Revenue YoY % (SPLY comparison)
  ROUND(
    (m.revenue - LAG(m.revenue, 12) OVER (ORDER BY m.month_start))
    / NULLIF(LAG(m.revenue, 12) OVER (ORDER BY m.month_start), 0) * 100, 1
  )                                                         AS revenue_yoy_pct
FROM monthly m
  JOIN ytd y ON m.year = y.year
ORDER BY m.month_start;

-- =====================================================
-- SECTION 5: MULTI-DATE MEASURES (DAX Display Folder: "Multi-Date")
-- Uses USERELATIONSHIP to activate inactive date relationships
-- =====================================================

-- Revenue by Approved Date
SELECT SUM(fs.item_total) AS revenue_by_approved_date
FROM olist.gold.fact_sales fs
  JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
  JOIN olist.gold.dim_date dd ON do.order_approved_date = dd.date;

-- Revenue by Carrier Pickup Date
SELECT SUM(fs.item_total) AS revenue_by_carrier_pickup_date
FROM olist.gold.fact_sales fs
  JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
  JOIN olist.gold.dim_date dd ON do.delivered_carrier_date = dd.date;

-- Revenue by Delivery Date
SELECT SUM(fs.item_total) AS revenue_by_delivery_date
FROM olist.gold.fact_sales fs
  JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
  JOIN olist.gold.dim_date dd ON do.delivered_customer_date = dd.date;

-- Revenue by Estimated Delivery Date
SELECT SUM(fs.item_total) AS revenue_by_estimated_delivery_date
FROM olist.gold.fact_sales fs
  JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
  JOIN olist.gold.dim_date dd ON do.estimated_delivery_date = dd.date;

-- Orders by Delivery Date
SELECT COUNT(DISTINCT fs.order_id) AS orders_by_delivery_date
FROM olist.gold.fact_sales fs
  JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
  JOIN olist.gold.dim_date dd ON do.delivered_customer_date = dd.date;

-- Orders by Approved Date
SELECT COUNT(DISTINCT fs.order_id) AS orders_by_approved_date
FROM olist.gold.fact_sales fs
  JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
  JOIN olist.gold.dim_date dd ON do.order_approved_date = dd.date;

-- Revenue YTD by Delivery Date (using USERELATIONSHIP on delivered_customer_date)
WITH monthly_delivery AS (
  SELECT
    dd.year,
    SUM(fs.item_total) AS revenue_ytd_by_delivery_date
  FROM olist.gold.fact_sales fs
    JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
    JOIN olist.gold.dim_date dd ON do.delivered_customer_date = dd.date
  GROUP BY dd.year
)
SELECT * FROM monthly_delivery;

-- Revenue YoY % by Delivery Date
WITH monthly_delivery AS (
  SELECT
    DATE_TRUNC('month', do.delivered_customer_date) AS month_start,
    SUM(fs.item_total) AS revenue
  FROM olist.gold.fact_sales fs
    JOIN olist.gold.dim_orders do ON fs.order_id = do.order_id
  WHERE do.delivered_customer_date IS NOT NULL
  GROUP BY DATE_TRUNC('month', do.delivered_customer_date)
)
SELECT
  month_start,
  revenue,
  LAG(revenue, 12) OVER (ORDER BY month_start) AS revenue_sply,
  ROUND(
    (revenue - LAG(revenue, 12) OVER (ORDER BY month_start))
    / NULLIF(LAG(revenue, 12) OVER (ORDER BY month_start), 0) * 100, 1
  ) AS revenue_yoy_pct_by_delivery_date
FROM monthly_delivery
ORDER BY month_start;

-- Avg Delivery Days by Delivery Date
SELECT
  ROUND(AVG(do.delivery_days), 2) AS avg_delivery_days_by_delivery_date
FROM olist.gold.dim_orders do
  JOIN olist.gold.dim_date dd ON do.delivered_customer_date = dd.date
WHERE do.delivery_days IS NOT NULL;

-- On-Time Rate by Delivery Date
SELECT
  ROUND(
    (COUNT(*) - SUM(CASE WHEN do.is_late THEN 1 ELSE 0 END))
    / NULLIF(COUNT(*), 0) * 100, 1
  ) AS on_time_rate_by_delivery_date
FROM olist.gold.dim_orders do
  JOIN olist.gold.dim_date dd ON do.delivered_customer_date = dd.date
WHERE do.is_late IS NOT NULL;

-- =====================================================
-- SECTION 6: ORIGINAL EXPLORATORY QUERIES (silver layer)
-- Kept for reference — these were the original aggregations
-- before the star schema was built.
-- =====================================================

-- Daily revenue summary with order and item counts
SELECT
  date(o.order_purchase_timestamp)                                   AS order_date,
  COUNT(DISTINCT o.order_id)                                         AS total_orders,
  COUNT(*)                                                           AS total_items,
  SUM(oi.price)                                                      AS total_revenue,
  SUM(oi.freight_value)                                              AS total_freight,
  SUM(oi.item_total)                                                 AS total_order_value,
  ROUND(SUM(oi.item_total) / COUNT(DISTINCT o.order_id), 2)          AS avg_order_value,
  SUM(CASE WHEN o.is_late THEN 1 ELSE 0 END)                          AS late_orders
FROM olist.silver.orders o
JOIN olist.silver.order_items oi ON o.order_id = oi.order_id
GROUP BY ALL;

-- Product category performance with revenue and review metrics
SELECT
  p.product_category,
  COUNT(DISTINCT oi.order_id)   AS total_orders,
  COUNT(*)                     AS total_items_sold,
  SUM(oi.price)                AS total_revenue,
  ROUND(AVG(oi.price), 2)      AS avg_item_price,
  ROUND(AVG(r.review_score), 2) AS avg_review_score
FROM olist.silver.order_items oi
JOIN olist.silver.products p     ON oi.product_id = p.product_id
LEFT JOIN olist.silver.order_reviews r ON oi.order_id = r.order_id
GROUP BY ALL;

-- Seller performance with revenue and review metrics
SELECT
  s.seller_id,
  s.city                       AS seller_city,
  s.state                      AS seller_state,
  COUNT(DISTINCT oi.order_id)  AS total_orders,
  COUNT(*)                     AS total_items_sold,
  SUM(oi.price)                AS total_revenue,
  SUM(oi.freight_value)        AS total_freight,
  ROUND(AVG(r.review_score), 2) AS avg_review_score
FROM olist.silver.order_items oi
JOIN olist.silver.sellers s      ON oi.seller_id = s.seller_id
LEFT JOIN olist.silver.order_reviews r ON oi.order_id = r.order_id
GROUP BY ALL;

-- Payment type distribution and installment metrics
SELECT
  payment_type,
  COUNT(DISTINCT order_id)     AS total_orders,
  SUM(payment_value)           AS total_payment_value,
  ROUND(AVG(payment_value), 2) AS avg_payment_value,
  ROUND(AVG(payment_installments), 1) AS avg_installments
FROM olist.silver.order_payments
GROUP BY ALL;

-- Customer order history and spend summary
SELECT
  c.customer_id,
  c.customer_unique_id,
  c.city,
  c.state,
  COUNT(DISTINCT o.order_id)                                          AS total_orders,
  SUM(oi.item_total)                                                 AS total_spend,
  ROUND(SUM(oi.item_total) / COUNT(DISTINCT o.order_id), 2)           AS avg_order_value,
  MAX(o.order_purchase_timestamp)                                    AS last_order_date,
  MIN(o.order_purchase_timestamp)                                    AS first_order_date,
  ROUND(AVG(r.review_score), 2)                                       AS avg_review_score
FROM olist.silver.customers c
JOIN olist.silver.orders o        ON c.customer_id = o.customer_id
JOIN olist.silver.order_items oi  ON o.order_id = oi.order_id
LEFT JOIN olist.silver.order_reviews r ON o.order_id = r.order_id
GROUP BY ALL;