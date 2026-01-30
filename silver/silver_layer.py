# Databricks notebook source
# MAGIC %md
# MAGIC ## INITIALIZATION

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## READ & SILVER TRANASFORM

# COMMAND ----------

from pyspark.sql.functions import col, upper, lower

customers_silver = (
    spark.table("workspace.bronze.customers_info")
    .dropDuplicates(["customer_id"])
    .withColumn("customer_city", lower(col("customer_city")))
    .withColumn("customer_state", upper(col("customer_state")))
)

customers_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.customers_clean"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## CLEAN GEOLOCATION_INFO

# COMMAND ----------

from pyspark.sql.functions import avg, first

geo_silver = (
    spark.table("workspace.bronze.geolocation_info")
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        avg("geolocation_lat").alias("lat"),
        avg("geolocation_lng").alias("lng"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state")
    )
)

geo_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.geolocation_clean"
)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks 

# COMMAND ----------

spark.table("workspace.silver.geolocation_clean").printSchema()
spark.table("workspace.silver.geolocation_clean").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDERS_INFO

# COMMAND ----------

from pyspark.sql.functions import col

orders_silver = (
    spark.table("workspace.bronze.orders_info")
    .dropDuplicates(["order_id"])
    .filter(col("order_status").isNotNull())
)

orders_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.orders_clean"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDERSITEM_CLEAN

# COMMAND ----------

from pyspark.sql.functions import col

order_items_silver = (
    spark.table("workspace.bronze.orderitems_info")
    .filter(col("price").isNotNull())
    .filter(col("freight_value").isNotNull())
)

order_items_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.order_items_clean"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDERSPAYMENT_CLEAN

# COMMAND ----------

from pyspark.sql.functions import col, lower

payments_silver = (
    spark.table("workspace.bronze.orderpayments_info")
    .filter(col("payment_value") > 0)
    .withColumn("payment_type", lower("payment_type"))
)

payments_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.payments_clean"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ODERREVIEWS_CLEAN

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.silver.reviews_clean;
# MAGIC
# MAGIC

# COMMAND ----------

spark.table("workspace.bronze.orderreviews_info").printSchema()


# COMMAND ----------

from pyspark.sql.functions import expr

reviews_silver = (
    spark.table("workspace.bronze.orderreviews_info")
    .select(
        "review_id",
        "order_id",
        expr("try_cast(review_score as int)").alias("review_score"),
        "review_comment_title",
        "review_comment_message",
        expr("try_cast(review_creation_date as timestamp)").alias("review_creation_date"),
        expr("try_cast(review_answer_timestamp as timestamp)").alias("review_answer_timestamp")
    )
    .filter("review_score between 1 and 5")
    .dropDuplicates(["review_id"])
)

reviews_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.reviews_clean"
)


# COMMAND ----------

spark.table("workspace.silver.reviews_clean").printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## PRODUCTS_CLEAN

# COMMAND ----------

products_silver = (
    spark.table("workspace.bronze.products_info")
    .filter("product_weight_g > 0")
)

products_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.products_clean"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## PRODUCT TRANSLATION_CLEAN

# COMMAND ----------

category_silver = (
    spark.table("workspace.bronze.producttranslation_info")
    .dropDuplicates(["product_category_name"])
)

category_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.category_translation_clean"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## SELLERS CLEAN

# COMMAND ----------

from pyspark.sql.functions import lower, upper

sellers_silver = (
    spark.table("workspace.bronze.sellers_info")
    .dropDuplicates(["seller_id"])
    .withColumn("seller_city", lower("seller_city"))
    .withColumn("seller_state", upper("seller_state"))
)

sellers_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.sellers_clean"
)
