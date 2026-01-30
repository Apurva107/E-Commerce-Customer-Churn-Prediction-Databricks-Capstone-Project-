# Databricks notebook source
# MAGIC %md
# MAGIC ## ML TRAINING (PySpark + MLflow)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.mlflow;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.mlflow.tmp;
# MAGIC

# COMMAND ----------

import os
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/mlflow/tmp"


# COMMAND ----------

[k for k in locals().keys() if "model" in k.lower()]


# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.mlflow;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.mlflow.model_artifacts;
# MAGIC

# COMMAND ----------

import os
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/mlflow/model_artifacts"


# COMMAND ----------

with mlflow.start_run() as run:
    mlflow.spark.log_model(
        spark_model=model,     
        artifact_path="model"
    )

    mlflow.log_param("model_type", "Spark ML")
    mlflow.log_metric("auc", auc)   # keep or remove if not available

    print("FINAL RUN ID:", run.info.run_id)


# COMMAND ----------

[k for k, v in locals().items() if "model" in k.lower()]


# COMMAND ----------

[k for k in locals().keys() if "df" in k.lower()]


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline


# COMMAND ----------

feature_cols = [
    "total_orders",
    "total_spend",
    "avg_order_value",
    "avg_review_score"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="churn_label"
)

pipeline = Pipeline(stages=[assembler, lr])


# COMMAND ----------

df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import datediff, current_date


# COMMAND ----------

ml_df = (
    df
    .withColumn(
        "recency_days",
        datediff(current_date(), df.last_order_date)
    )
    .drop("customer_unique_id", "last_order_date")
)


# COMMAND ----------

ml_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Train/test split

# COMMAND ----------

train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build pipeline

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

feature_cols = [
    "total_orders",
    "total_spend",
    "avg_order_value",
    "avg_review_score",
    "recency_days"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="churn_label", maxIter=20)

pipeline = Pipeline(stages=[assembler, lr])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Pipeline

# COMMAND ----------

pipeline_model = pipeline.fit(train_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

predictions = pipeline_model.transform(test_df)

evaluator = BinaryClassificationEvaluator(labelCol="churn_label", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
print("AUC:", auc)


# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow logging

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
import os

mlflow.set_registry_uri("databricks-uc")
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/mlflow/model_artifacts"

input_example = train_df.limit(5).drop("churn_label").toPandas()
signature = infer_signature(input_example, pipeline_model.transform(train_df.limit(5)))

with mlflow.start_run() as run:
    mlflow.spark.log_model(
        spark_model=pipeline_model,
        artifact_path="model",
        signature=signature,
        input_example=input_example
    )
    print("FINAL UC RUN ID:", run.info.run_id)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model

# COMMAND ----------

mlflow.register_model(
    model_uri=f"runs:/{run.info.run_id}/model",
    name="workspace.mlflow.ecommerce_churn_model"
)
