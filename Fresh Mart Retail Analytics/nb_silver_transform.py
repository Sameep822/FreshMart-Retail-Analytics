# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==========================
# Read Bronze Tables
# ==========================

orders_df = spark.table("workspace.default.bronze_raw_orders")
order_items_df = spark.table("workspace.default.bronze_raw_order_items")
customers_df = spark.table("workspace.default.bronze_raw_customers")
delivery_df = spark.table("workspace.default.bronze_raw_delivery_logs")

# ==========================
# ORDERS
# ==========================

orders_df = (
    orders_df
    .dropDuplicates(["order_id"])
    .withColumn(
        "order_date",
        to_timestamp(col("order_date"), "dd-MM-yyyy HH:mm")
    )
)

# ==========================
# ORDER ITEMS
# ==========================

order_items_df = (
    order_items_df
    .dropDuplicates(["item_id"])
    .withColumn("qty", col("qty").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("discount", col("discount").cast("double"))
    .withColumn(
        "order_total",
        (col("qty") * col("unit_price")) - col("discount")
    )
)

# ==========================
# CUSTOMERS
# ==========================

customers_df = (
    customers_df
    .dropDuplicates(["customer_id"])
    .withColumn("email_hash", sha2(col("email"), 256))
    .withColumn("phone_hash", sha2(col("phone"), 256))
    .drop("email", "phone")
)

# ==========================
# DELIVERY
# ==========================

delivery_df = (
    delivery_df
    .replace("NULL", None)
    .dropDuplicates(["delivery_id"])
    .withColumn(
        "pickup_time",
        to_timestamp(col("pickup_time"), "dd-MM-yyyy HH:mm")
    )
    .withColumn(
        "delivery_time",
        to_timestamp(col("delivery_time"), "dd-MM-yyyy HH:mm")
    )
    .withColumn(
        "delivery_duration_mins",
        (unix_timestamp(col("delivery_time")) - unix_timestamp(col("pickup_time"))) / 60
    )
    .withColumn(
        "is_incomplete",
        when(col("delivery_time").isNull(), True).otherwise(False)
    )
)

# ==========================
# SAVE SILVER TABLES
# ==========================

orders_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.silver_orders")

order_items_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.silver_order_items")

customers_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.silver_customers")

delivery_df.write.mode("overwrite").format("delta").saveAsTable("workspace.default.silver_delivery_logs")

print("====================================")
print(" Silver Layer Created Successfully ")
print("====================================")