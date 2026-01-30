# Databricks notebook source
# MAGIC %md
# MAGIC ## gold.customer_order_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.customer_order_summary AS
# MAGIC WITH order_value AS (
# MAGIC     SELECT
# MAGIC         oi.order_id,
# MAGIC         SUM(oi.price + oi.freight_value) AS order_total
# MAGIC     FROM workspace.silver.order_items_clean oi
# MAGIC     GROUP BY oi.order_id
# MAGIC ),
# MAGIC
# MAGIC customer_orders AS (
# MAGIC     SELECT
# MAGIC         c.customer_unique_id,
# MAGIC         o.order_id,
# MAGIC         o.order_purchase_timestamp
# MAGIC     FROM workspace.silver.orders_clean o
# MAGIC     JOIN workspace.silver.customers_clean c
# MAGIC         ON o.customer_id = c.customer_id
# MAGIC ),
# MAGIC
# MAGIC reviews AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         review_score
# MAGIC     FROM workspace.silver.reviews_clean
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     co.customer_unique_id,
# MAGIC     COUNT(DISTINCT co.order_id) AS total_orders,
# MAGIC     SUM(ov.order_total) AS total_spend,
# MAGIC     AVG(ov.order_total) AS avg_order_value,
# MAGIC     AVG(r.review_score) AS avg_review_score,
# MAGIC     MAX(co.order_purchase_timestamp) AS last_order_date,
# MAGIC     CASE
# MAGIC         WHEN MAX(co.order_purchase_timestamp) < date_sub(current_date(), 180)
# MAGIC         THEN 1 ELSE 0
# MAGIC     END AS churn_label
# MAGIC FROM customer_orders co
# MAGIC JOIN order_value ov ON co.order_id = ov.order_id
# MAGIC LEFT JOIN reviews r ON co.order_id = r.order_id
# MAGIC GROUP BY co.customer_unique_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.seller_performance_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.seller_performance_summary AS
# MAGIC SELECT
# MAGIC     oi.seller_id,
# MAGIC     COUNT(DISTINCT oi.order_id) AS total_orders,
# MAGIC     SUM(oi.price) AS total_revenue,
# MAGIC     AVG(r.review_score) AS avg_review_score
# MAGIC FROM workspace.silver.order_items_clean oi
# MAGIC LEFT JOIN workspace.silver.reviews_clean r
# MAGIC     ON oi.order_id = r.order_id
# MAGIC GROUP BY oi.seller_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.product_demand_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.product_demand_summary AS
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC     COUNT(oi.order_id) AS order_count,
# MAGIC     SUM(oi.price) AS total_revenue,
# MAGIC     AVG(r.review_score) AS avg_review_score
# MAGIC FROM workspace.silver.order_items_clean oi
# MAGIC JOIN workspace.silver.products_clean p
# MAGIC     ON oi.product_id = p.product_id
# MAGIC LEFT JOIN workspace.silver.reviews_clean r
# MAGIC     ON oi.order_id = r.order_id
# MAGIC GROUP BY p.product_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_churn_features_job

# COMMAND ----------

from pyspark.sql.functions import *

churn_df = spark.sql("""
SELECT
  c.customer_unique_id,
  COUNT(DISTINCT o.order_id)                    AS total_orders,
  SUM(p.payment_value)                          AS total_spend,
  AVG(p.payment_value)                          AS avg_order_value,
  AVG(r.review_score)                           AS avg_review_score,
  DATEDIFF(CURRENT_DATE, MAX(o.order_purchase_timestamp)) AS recency_days,
  CASE
    WHEN DATEDIFF(CURRENT_DATE, MAX(o.order_purchase_timestamp)) > 180 THEN 1
    ELSE 0
  END AS churn_label
FROM silver.customers_clean c
LEFT JOIN silver.orders_clean o ON c.customer_id = o.customer_id
LEFT JOIN silver.payments_clean p ON o.order_id = p.order_id
LEFT JOIN silver.reviews_clean r ON o.order_id = r.order_id
GROUP BY c.customer_unique_id
""")

churn_df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("gold.customer_churn_features")
