# Databricks notebook source
# 1. Load Bronze
customers = spark.table("workspace.bronze.customers_info")

# 2. Cast columns, handle errors
from pyspark.sql.functions import col

customers_silver = customers.select(
    col("customer_id").cast("string"),
    col("customer_unique_id").cast("string"),
    col("customer_zip_code_prefix").cast("int"),
    col("customer_city").cast("string"),
    col("customer_state").cast("string")
)

# 3. Write Silver
customers_silver.write.mode("overwrite").saveAsTable("workspace.silver.customers_info")
