# Databricks notebook source
# Read the data from the table
df = spark.table("merch_finance_proj1_nonprd.bronze.emp")

# Clean the data: capitalize first letter of all columns except 'company' and remove extra spaces in names
from pyspark.sql.functions import col, initcap, trim, lower

cleaned_df = df.select([
    initcap(trim(col(c))).alias(c.capitalize()) if c != 'company' 
    else lower(trim(col(c))).alias(c) 
    for c in df.columns
])

# Display the cleaned dataframe


# COMMAND ----------

from pyspark.sql.functions import concat_ws, lit, lower

# Generate full name and email columns
result_df = cleaned_df.withColumn(
    "full_name", concat_ws(" ", col("First_name"), col("Last_name"))
).withColumn(
    "email", concat_ws(".", lower(col("First_name")), lower(col("Last_name"))).alias("email")
).withColumn(
    "email", concat_ws("", col("email"), lit("@"), col("company"), lit(".com"))
)

# Display the result dataframe
display(result_df)

# COMMAND ----------

# Write the result dataframe to the silver schema
result_df.write.mode("overwrite").saveAsTable("merch_finance_proj1_nonprd.silver.emp_cleansed")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from merch_finance_proj1_nonprd.silver.emp_cleansed

# COMMAND ----------


