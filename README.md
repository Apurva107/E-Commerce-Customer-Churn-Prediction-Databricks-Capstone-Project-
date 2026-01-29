# E-Commerce Customer Churn Prediction | Databricks Capstone Project

## Project Overview

This project predicts **customer churn** in an e-commerce platform using historical customer, order, product, and review data. By identifying customers likely to churn, businesses can implement **targeted retention strategies**, increasing customer lifetime value and revenue.

The project demonstrating **end-to-end data engineering, analytics, machine learning, and workflow orchestration** using Databricks.

---

## Dataset

- **Source:** [Olist E-Commerce Public Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)  
- **Files:** Customers, Orders, Order Items, Products, Payments, Reviews, Sellers, Geolocation, Product Category Translations  

**Key challenges in the dataset:**
- Missing or inconsistent data  
- Malformed timestamps  
- Multiple tables requiring cleaning and joining  

---

## Project Architecture

The project follows the **Medallion Architecture** using **Delta Lake**:

### Bronze Layer
- Raw CSVs loaded into Delta tables  
- Preserves original data with ACID compliance  

### Silver Layer
- Cleaned and transformed tables  
- Handled missing values, deduplication, type corrections  
- Standardized timestamps and normalized columns  

### Gold Layer
- Aggregated customer features for ML:  
  - Total orders, total spend, average order value  
  - Average review score  
  - Recency of last order  
  - Churn label creation  

---

## Machine Learning Pipeline

- **Task:** Binary classification (churn prediction)  
- **Features:** Aggregated customer behavior metrics  
- **Model Tracking:** MLflow for experiment tracking and artifact management  
- **Model Registration:** Unity Catalog for production-ready deployment  
- **Outcome:** Predictive model identifying high-risk churn customers  

---

## Workflow Orchestration

- **Databricks Jobs** automate the pipeline:  
  1. Bronze Job – load raw data  
  2. Silver Job – clean and transform data  
  3. Gold Job – aggregate features  
  4. MLflow Job – train and register models  

- Automation ensures **reproducibility** and enables **scheduled retraining**  

---

## Analytics & Business Insights

- Identified customers at risk of churn  
- Segmented top-spending and high-value customers  
- Provided actionable insights for **retention campaigns** and **loyalty programs**  
- Measured metrics like **average order value, purchase frequency, and review scores**  

---

## Tools & Technologies

- **Databricks / Delta Lake:** Data storage & medallion architecture  
- **PySpark / Spark SQL:** Data cleaning, transformation, and feature engineering  
- **MLflow:** Model tracking, versioning, and registration  
- **Python:** Workflow scripting and ML pipeline  
- **Databricks Jobs:** Pipeline orchestration  
- **Unity Catalog:** Secure model registration  

---

## Project Folder Structure
E-commerce Project/

- ├── bronze/ # Raw Delta tables
- ├── silver/ # Cleaned & transformed tables
- ├── gold/ # Aggregated ML-ready features
- ├── mlflow/ # Tracked ML models and artifacts
- ├── notebooks/ # ETL & ML pipeline notebooks

-----------


---

## Conclusion

This project demonstrates a **full end-to-end Data + AI pipeline**:

- From raw data ingestion → cleaning → aggregation → ML model training → automated orchestration  
- Combines **data engineering, machine learning, and business analytics**  
- Produces **actionable insights** and a **predictive model** for churn  

