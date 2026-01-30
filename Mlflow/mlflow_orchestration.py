# Databricks notebook source
# Import libraries
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature

# Set MLflow experiment
mlflow.set_experiment("/Users/apurvayp0710@gmail.com/E-commerce project/ML Flow/mlflow")


# COMMAND ----------

# Load Gold table
gold_df = spark.table("workspace.gold.customer_churn_features")

# Select feature columns and label
feature_cols = ["total_orders", "total_spend", "avg_order_value", "avg_review_score", "recency_days"]
label_col = "churn_label"

# Split train/test
train_df, test_df = gold_df.randomSplit([0.8, 0.2], seed=42)


# COMMAND ----------

# Vector assembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol=label_col)

# Pipeline
pipeline = Pipeline(stages=[assembler, lr])


# COMMAND ----------

with mlflow.start_run() as run:
    
    # Train model
    pipeline_model = pipeline.fit(train_df)
    
    # Make predictions on test set
    predictions = pipeline_model.transform(test_df)
    
    # Calculate metrics
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    
    evaluator = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="prediction")
    auc = evaluator.evaluate(predictions)
    print(f"AUC: {auc}")
    
    # Log metrics
    mlflow.log_metric("AUC", auc)
    
    # Infer model signature
    input_example = test_df.select(feature_cols).limit(5)
    signature = infer_signature(input_example.toPandas(), predictions.select("prediction").toPandas())
    
    # Log Spark ML model
    mlflow.spark.log_model(
        pipeline_model,
        artifact_path="model",
        signature=signature,
        input_example=input_example
    )
    
    print("Run ID:", run.info.run_id)


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.customer_order_summary;
# MAGIC