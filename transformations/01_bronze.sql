-- =====================================================
-- BRONZE: raw ingestion, no cleaning
-- =====================================================

CREATE OR REFRESH STREAMING TABLE olist.bronze.customers COMMENT 'Raw customers' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
      '/Volumes/olist/bronze/raw_files/customers/',
      format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE olist.bronze.orders COMMENT 'Raw orders' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/orders/',
  format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE olist.bronze.order_items COMMENT 'Raw order items' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/order_items/',
  format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE olist.bronze.order_payments COMMENT 'Raw order payments' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/order_payments/',
  format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE olist.bronze.order_reviews COMMENT 'Raw order reviews' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/order_reviews/',
  format => 'csv', header => true, inferColumnTypes => false,
  multiLine => true, escape => '"');

CREATE OR REFRESH STREAMING TABLE olist.bronze.products COMMENT 'Raw products' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/products/',
  format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE olist.bronze.sellers COMMENT 'Raw sellers' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/sellers/',
  format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE olist.bronze.geolocation COMMENT 'Raw geolocation' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/geolocation/',
  format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE olist.bronze.category_translation COMMENT 'Raw category name translation' AS
SELECT *,
     current_timestamp() AS _ingested_at,
     _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/olist/bronze/raw_files/category_translation/',
  format => 'csv', header => true, inferColumnTypes => false);