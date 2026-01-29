# Databricks notebook source
# MAGIC %md
# MAGIC ## Read From CSV and Write Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "customer_dataset.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_customers_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.customers_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "geolocation_dataset.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_geolocation_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.geolocation_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "order_items.csv"

# COMMAND ----------

# DBTITLE 1,Cell 7
df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_order_items_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.orderitems_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "order_payment.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_order_payments_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.orderpayments_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "order_reviews.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_order_reviews_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.orderreviews_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "orders_dataset.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_orders_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.orders_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "products_dataset.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_products_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.products_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load "sellers_dataset.csv"

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/olist_sellers_dataset.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.sellers_info")

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv('/Volumes/workspace/bronze/raw_sources/archive (4)/product_category_name_translation.csv', header=True)
df.write.mode("overwrite").saveAsTable("workspace.bronze.producttranslation_info")