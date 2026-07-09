# Databricks notebook source
from pyspark.sql.functions import current_date, lit

# ==========================
# File Paths
# ==========================

orders_path = "/Volumes/workspace/default/freshmart_data/orders.csv"
order_items_path = "/Volumes/workspace/default/freshmart_data/order_items.csv"
customers_path = "/Volumes/workspace/default/freshmart_data/customers.json"
delivery_path = "/Volumes/workspace/default/freshmart_data/delivery.csv"

# ==========================
# Read Files
# ==========================

orders_df = spark.read.option("header", True).csv(orders_path)

order_items_df = spark.read.option("header", True).csv(order_items_path)

customers_df = spark.read.option("multiline", True).json(customers_path)

delivery_df = spark.read.option("header", True).csv(delivery_path)

# ==========================
# Add Metadata Columns
# ==========================

orders_df = (
    orders_df
    .withColumn("_ingested_date", current_date())
    .withColumn("_source_file", lit("orders.csv"))
)

order_items_df = (
    order_items_df
    .withColumn("_ingested_date", current_date())
    .withColumn("_source_file", lit("order_items.csv"))
)

customers_df = (
    customers_df
    .withColumn("_ingested_date", current_date())
    .withColumn("_source_file", lit("customers.json"))
)

delivery_df = (
    delivery_df
    .withColumn("_ingested_date", current_date())
    .withColumn("_source_file", lit("delivery.csv"))
)

# ==========================
# Create Schema
# ==========================

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")

# ==========================
# Save Delta Tables
# ==========================

orders_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.bronze_raw_orders")

order_items_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.bronze_raw_order_items")

customers_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.bronze_raw_customers")

delivery_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.bronze_raw_delivery_logs")

print("========================================")
print(" Bronze Layer Created Successfully ")
print("========================================")