# 🛒 FreshMart Retail Analytics using PySpark & Delta Lake

## 📌 Project Overview

FreshMart Retail Analytics is an end-to-end Batch ETL Data Pipeline developed using **PySpark**, **Delta Lake**, and **Databricks**. The pipeline processes raw retail data from multiple sources, cleans and transforms it, and generates business-ready analytical reports following the **Medallion Architecture (Bronze → Silver → Gold)**.

This project demonstrates a modern data engineering workflow for retail analytics using scalable big data technologies.

---

## 🎯 Problem Statement

FreshMart is an online grocery delivery platform operating in multiple cities. Every day, the platform generates large volumes of order, customer, and delivery data in CSV and JSON format.

The objective of this project is to transform raw data into clean, analytics-ready datasets and generate meaningful business insights such as:

- Daily Revenue by City
- Product Return Analysis
- Delivery Performance
- Customer Purchase Summary

---

# 🏗️ Architecture

```
                Raw Files
      (CSV / JSON Datasets)
               │
               ▼
        🥉 Bronze Layer
   Raw Data Ingestion (Delta)
               │
               ▼
        🥈 Silver Layer
 Data Cleaning & Transformation
               │
               ▼
         🥇 Gold Layer
 Business Analytics & Reports
```

---

# 📂 Dataset

The project uses four datasets:

| Dataset | Format |
|----------|---------|
| Orders | CSV |
| Order Items | CSV |
| Customers | JSON |
| Delivery Logs | CSV |

---

# 🛠️ Technologies Used

- Databricks
- PySpark
- Delta Lake
- Spark SQL
- Python
- Medallion Architecture

---

# 📁 Project Structure

```
FreshMart-Retail-Analytics/

│
├── orders.csv
├── order_items.csv
├── customers.json
├── delivery.csv
│
├── nb_bronze_ingest
├── nb_silver_transform
├── nb_gold_aggregate
├── nb_orchestrator
│
└── README.md
```

---

# 🥉 Bronze Layer

### Objective

Ingest raw files without modifying the data.

### Tasks Performed

- Read CSV & JSON files
- Added metadata columns
  - `_ingested_date`
  - `_source_file`
- Stored data as Delta Tables

### Output Tables

- bronze_raw_orders
- bronze_raw_order_items
- bronze_raw_customers
- bronze_raw_delivery_logs

---

# 🥈 Silver Layer

### Objective

Clean and standardize the raw data.

### Transformations

- Removed duplicate records
- Converted data types
- Handled NULL values
- Calculated Order Total
- Calculated Delivery Duration
- Applied SHA-256 hashing on:
  - Email
  - Phone Number

### Output Tables

- silver_orders
- silver_order_items
- silver_customers
- silver_delivery_logs

---

# 🥇 Gold Layer

Business-ready aggregated reports were generated.

## Reports

### 1. Daily Revenue by City

- Revenue
- Order Count
- Average Basket Size

---

### 2. Product Return Summary

- Returned Products
- Category-wise Returns

---

### 3. Delivery Zone Performance

- Average Delivery Time
- Failed Deliveries
- Total Deliveries

---

### 4. Customer Summary

- Total Spend
- Order Count
- Last Order Date

---

# 🔄 Pipeline Workflow

```
Raw CSV / JSON Files
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
Gold Layer
        │
        ▼
Business Reports
```

---

# 🚀 Execution Steps

### Step 1

Run:

```
nb_bronze_ingest
```

---

### Step 2

Run:

```
nb_silver_transform
```

---

### Step 3

Run:

```
nb_gold_aggregate
```

---

### Step 4

(Optional)

Run:

```
nb_orchestrator
```

to execute the complete pipeline sequentially.

---

# 📊 Business Insights Generated

✔ Daily Revenue by City

✔ Product Return Analysis

✔ Delivery Performance Dashboard

✔ Customer Purchase Summary

---

# 📈 Features

- End-to-End ETL Pipeline
- Batch Data Processing
- Delta Lake Storage
- Data Cleaning
- Duplicate Removal
- Null Handling
- PII Masking
- Business Aggregation
- Scalable Medallion Architecture

---

# 📌 Learning Outcomes

This project demonstrates practical implementation of:

- PySpark DataFrames
- Delta Lake
- ETL Pipeline Design
- Medallion Architecture
- Data Cleaning
- Spark SQL
- Batch Processing
- Data Engineering Best Practices

---

# 👨‍💻 Author

**Sameep Bhutani**

B.Tech Computer Science Engineering

Maharishi Markandeshwar (Deemed to be University)

---

# ⭐ Future Enhancements

- Incremental Data Loading
- Streaming Pipeline using Structured Streaming
- Power BI Dashboard Integration
- Apache Airflow Orchestration
- Automated Data Quality Validation
- Cloud Deployment on Azure/AWS

---

# 📜 License

This project is developed for educational and learning purposes.
