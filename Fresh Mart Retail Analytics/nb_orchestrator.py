# Databricks notebook source
# MAGIC %run ./nb_bronze_ingest

# COMMAND ----------

# MAGIC %run ./nb_silver_transform

# COMMAND ----------

# MAGIC %run ./nb_gold_aggregate

# COMMAND ----------

print("===================================")
print(" FreshMart Pipeline Executed Successfully ")
print("===================================")

# COMMAND ----------

spark.table("workspace.default.gold_daily_revenue_by_city").show()

# COMMAND ----------

spark.table("workspace.default.gold_product_return_summary").show()

# COMMAND ----------

spark.table("workspace.default.gold_delivery_zone_performance").show()

# COMMAND ----------

spark.table("workspace.default.gold_customer_summary").show()