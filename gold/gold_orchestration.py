# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.silver;
# MAGIC

# COMMAND ----------

df = spark.table("workspace.silver.customers_clean")
df.printSchema()
df.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, count, max, datediff, current_date, when

# Load Silver tables
customers = spark.table("workspace.silver.customers_clean").alias("c")
orders = spark.table("workspace.silver.orders_clean").alias("o")
order_items = spark.table("workspace.silver.order_items_clean").alias("oi")
payments = spark.table("workspace.silver.payments_clean").alias("p")
reviews = spark.table("workspace.silver.reviews_clean").alias("r")
products = spark.table("workspace.silver.products_clean").alias("pr")
sellers = spark.table("workspace.silver.sellers_clean").alias("s")
category_translation = spark.table("workspace.silver.category_translation_clean").alias("ct")
geolocation = spark.table("workspace.silver.geolocation_clean").alias("g")



# COMMAND ----------

# MAGIC %md
# MAGIC ## JOIN TABLES

# COMMAND ----------

gold_df = (
    orders.join(customers, orders.customer_id == customers.customer_id, "inner")
          .join(order_items, orders.order_id == order_items.order_id, "left")
          .join(payments, orders.order_id == payments.order_id, "left")
          .join(reviews, orders.order_id == reviews.order_id, "left")
          .join(products, order_items.product_id == products.product_id, "left")
          .join(sellers, order_items.seller_id == sellers.seller_id, "left")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## AGGREGATE FEATURE

# COMMAND ----------

# Feature engineering per customer
gold_features = (
    gold_df.groupBy("c.customer_unique_id")
    .agg(
        count("o.order_id").alias("total_orders"),
        sum("p.payment_value").alias("total_spend"),
        avg("oi.price").alias("avg_order_value"),
        avg("r.review_score").alias("avg_review_score"),
        max("o.order_purchase_timestamp").alias("last_order_date")
    )
)

# Add churn label (1 if last order > 180 days ago)
gold_features = gold_features.withColumn(
    "churn_label",
    when(datediff(current_date(), col("last_order_date")) > 180, 1).otherwise(0)
)

# Add recency_days
gold_features = gold_features.withColumn(
    "recency_days",
    datediff(current_date(), col("last_order_date"))
)


# COMMAND ----------

gold_features.printSchema()
gold_features.show(5, truncate=False)
