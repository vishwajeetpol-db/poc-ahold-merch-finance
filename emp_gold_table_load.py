# Databricks notebook source
emp_cleansed_df = spark.table("merch_finance_proj1_nonprd.silver.emp_cleansed")
aggregated_df = emp_cleansed_df.groupBy("company").count()
aggregated_df.write.mode("overwrite").saveAsTable("merch_finance_proj1_nonprd.gold.emp_agg")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from merch_finance_proj1_nonprd.gold.emp_agg

# COMMAND ----------


