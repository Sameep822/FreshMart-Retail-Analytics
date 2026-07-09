# Databricks notebook source
from pyspark.sql.functions import *

# ==========================
# Read Silver Tables
# ==========================

orders = spark.table("workspace.default.silver_orders")
order_items = spark.table("workspace.default.silver_order_items")
customers = spark.table("workspace.default.silver_customers")
delivery = spark.table("workspace.default.silver_delivery_logs")

# ==========================
# Join Orders + Order Items
# ==========================

sales = orders.join(order_items, "order_id", "inner")

# ==========================
# 1. Daily Revenue By City
# ==========================

daily_revenue = (
    sales
    .groupBy(to_date("order_date").alias("order_date"), "city")
    .agg(
        round(sum("order_total"),2).alias("daily_revenue"),
        countDistinct("order_id").alias("total_orders"),
        round(avg("order_total"),2).alias("avg_basket_size")
    )
)

# ==========================
# 2. Product Return Summary
# ==========================

returns = (
    sales
    .filter(col("status")=="returned")
    .groupBy("product_name","category")
    .agg(
        count("*").alias("return_count")
    )
)

# ==========================
# 3. Delivery Zone Performance
# ==========================

delivery_perf = (
    delivery
    .groupBy("zone")
    .agg(
        round(avg("delivery_duration_mins"),2).alias("avg_delivery_time"),
        sum(
            when(col("status")=="failed",1).otherwise(0)
        ).alias("failed_deliveries"),
        count("*").alias("total_deliveries")
    )
)

# ==========================
# 4. Customer Summary
# ==========================

customer_summary = (
    sales
    .groupBy("customer_id")
    .agg(
        round(sum("order_total"),2).alias("total_spend"),
        countDistinct("order_id").alias("order_count"),
        max("order_date").alias("last_order")
    )
)

# ==========================
# Save Gold Tables
# ==========================

daily_revenue.write.mode("overwrite").format("delta").saveAsTable(
"workspace.default.gold_daily_revenue_by_city"
)

returns.write.mode("overwrite").format("delta").saveAsTable(
"workspace.default.gold_product_return_summary"
)

delivery_perf.write.mode("overwrite").format("delta").saveAsTable(
"workspace.default.gold_delivery_zone_performance"
)

customer_summary.write.mode("overwrite").format("delta").saveAsTable(
"workspace.default.gold_customer_summary"
)

print("====================================")
print(" Gold Layer Created Successfully ")
print("====================================")