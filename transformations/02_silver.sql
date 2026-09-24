-- =====================================================
-- SILVER: typed, cleaned, deduplicated, validated
-- =====================================================

CREATE OR REFRESH MATERIALIZED VIEW olist.silver.customers (
  CONSTRAINT customer_id_not_null        EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT customer_unique_id_not_null EXPECT (customer_unique_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_state_code            EXPECT (length(state) = 2)
)
COMMENT 'Cleaned customers, one row per customer_id'
AS
SELECT DISTINCT
  trim(customer_id)                             AS customer_id,
  trim(customer_unique_id)                      AS customer_unique_id,
  lpad(trim(customer_zip_code_prefix), 5, '0')  AS zip_code_prefix,
  initcap(trim(customer_city))                  AS city,
  upper(trim(customer_state))                   AS state
FROM olist.bronze.customers;


CREATE OR REFRESH MATERIALIZED VIEW olist.silver.orders (
  CONSTRAINT order_id_not_null      EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT customer_id_not_null   EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT purchase_ts_present    EXPECT (order_purchase_timestamp IS NOT NULL),
  CONSTRAINT delivered_has_date     EXPECT (NOT (order_status = 'delivered' AND order_delivered_customer_date IS NULL)),
  CONSTRAINT delivery_after_purchase EXPECT (order_delivered_customer_date IS NULL
                                             OR order_delivered_customer_date >= order_purchase_timestamp)
)
COMMENT 'Cleaned orders with derived delivery metrics'
AS
SELECT
  order_id,
  customer_id,
  lower(trim(order_status))                              AS order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  datediff(order_delivered_customer_date, order_purchase_timestamp)       AS delivery_days,
  datediff(order_delivered_customer_date, order_estimated_delivery_date)  AS delivery_delay_days,
  CASE WHEN order_delivered_customer_date IS NULL THEN NULL
       WHEN order_delivered_customer_date > order_estimated_delivery_date THEN TRUE
       ELSE FALSE
       END                                             AS is_late
FROM (
  SELECT
    trim(order_id)    AS order_id,
    trim(customer_id) AS customer_id,
    order_status,
    to_timestamp(order_purchase_timestamp)       AS order_purchase_timestamp,
    to_timestamp(order_approved_at)              AS order_approved_at,
    to_timestamp(order_delivered_carrier_date)   AS order_delivered_carrier_date,
    to_timestamp(order_delivered_customer_date)  AS order_delivered_customer_date,
    to_timestamp(order_estimated_delivery_date)  AS order_estimated_delivery_date,
    row_number() OVER (PARTITION BY order_id ORDER BY `_ingested_at` DESC) AS rn
  FROM olist.bronze.orders
)
WHERE rn = 1;


CREATE OR REFRESH MATERIALIZED VIEW olist.silver.order_items (
  CONSTRAINT order_id_not_null    EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT product_id_not_null  EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT seller_id_not_null   EXPECT (seller_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT price_non_negative   EXPECT (price >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT freight_non_negative EXPECT (freight_value >= 0)
)
COMMENT 'Cleaned order items, one row per order_id + order_item_id'
AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  shipping_limit_date,
  price,
  freight_value,
  price + freight_value AS item_total
FROM (
  SELECT
    trim(order_id)                        AS order_id,
    CAST(order_item_id AS INT)            AS order_item_id,
    trim(product_id)                      AS product_id,
    trim(seller_id)                       AS seller_id,
    to_timestamp(shipping_limit_date)     AS shipping_limit_date,
    CAST(price AS DECIMAL(12,2))          AS price,
    CAST(freight_value AS DECIMAL(12,2))  AS freight_value,
    row_number() OVER (PARTITION BY order_id, order_item_id ORDER BY _ingested_at DESC) AS rn
  FROM olist.bronze.order_items
)
WHERE rn = 1;


CREATE OR REFRESH MATERIALIZED VIEW olist.silver.order_payments (
  CONSTRAINT order_id_not_null     EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT payment_value_valid   EXPECT (payment_value >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT known_payment_type    EXPECT (payment_type IN ('credit_card','boleto','voucher','debit_card')) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned payments, one row per order_id + payment_sequential; drops undefined payment types'
AS
SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
FROM (
  SELECT
    trim(order_id)                              AS order_id,
    CAST(payment_sequential AS INT)             AS payment_sequential,
    lower(trim(payment_type))                   AS payment_type,
    CAST(payment_installments AS INT)           AS payment_installments,
    CAST(payment_value AS DECIMAL(12,2))        AS payment_value,
    row_number() OVER (PARTITION BY order_id, payment_sequential ORDER BY _ingested_at DESC) AS rn
  FROM olist.bronze.order_payments
)
WHERE rn = 1;


CREATE OR REFRESH MATERIALIZED VIEW olist.silver.order_reviews (
  CONSTRAINT review_id_not_null EXPECT (review_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT order_id_not_null  EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT score_in_range     EXPECT (review_score BETWEEN 1 AND 5) ON VIOLATION DROP ROW
)
COMMENT 'One review per order (latest); free-text comments excluded'
AS
SELECT review_id, order_id, review_score, review_creation_date, review_answer_timestamp
FROM (
  SELECT
    trim(review_id)                             AS review_id,
    trim(order_id)                              AS order_id,
    try_cast(review_score AS INT)               AS review_score,
    try_cast(review_creation_date AS TIMESTAMP) AS review_creation_date,
    try_cast(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp,
    row_number() OVER (PARTITION BY order_id
                       ORDER BY try_cast(review_answer_timestamp AS TIMESTAMP) DESC) AS rn
  FROM olist.bronze.order_reviews
)
WHERE rn = 1;


CREATE OR REFRESH MATERIALIZED VIEW olist.silver.products (
  CONSTRAINT product_id_not_null EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT category_present    EXPECT (product_category <> 'unknown')
)
COMMENT 'Products with English category names'
AS
SELECT
  trim(p.product_id) AS product_id,
  coalesce(t.product_category_name_english, trim(p.product_category_name), 'unknown') AS product_category,
  CAST(p.product_weight_g AS DOUBLE)   AS weight_g,
  CAST(p.product_length_cm AS DOUBLE)  AS length_cm,
  CAST(p.product_height_cm AS DOUBLE)  AS height_cm,
  CAST(p.product_width_cm AS DOUBLE)   AS width_cm,
  CAST(p.product_photos_qty AS INT)    AS photos_qty,
  CAST(p.product_length_cm AS DOUBLE) * CAST(p.product_height_cm AS DOUBLE)
    * CAST(p.product_width_cm AS DOUBLE) AS volume_cm3
FROM (SELECT DISTINCT * EXCEPT (_ingested_at, _source_file) FROM olist.bronze.products) p
LEFT JOIN (SELECT DISTINCT product_category_name, product_category_name_english
           FROM olist.bronze.category_translation) t
  ON trim(p.product_category_name) = trim(t.product_category_name);


CREATE OR REFRESH MATERIALIZED VIEW olist.silver.sellers (
  CONSTRAINT seller_id_not_null EXPECT (seller_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_state_code   EXPECT (length(state) = 2)
)
COMMENT 'Cleaned sellers'
AS
SELECT DISTINCT
  trim(seller_id)                              AS seller_id,
  lpad(trim(seller_zip_code_prefix), 5, '0')   AS zip_code_prefix,
  initcap(trim(seller_city))                   AS city,
  upper(trim(seller_state))                    AS state
FROM olist.bronze.sellers;


CREATE OR REFRESH MATERIALIZED VIEW olist.silver.geolocation
COMMENT 'One row per zip prefix (averaged coordinates)'
AS
SELECT
  lpad(trim(geolocation_zip_code_prefix), 5, '0') AS zip_code_prefix,
  avg(CAST(geolocation_lat AS DOUBLE))            AS latitude,
  avg(CAST(geolocation_lng AS DOUBLE))            AS longitude,
  first(initcap(trim(geolocation_city)))          AS city,
  first(upper(trim(geolocation_state)))           AS state
FROM olist.bronze.geolocation
GROUP BY lpad(trim(geolocation_zip_code_prefix), 5, '0');