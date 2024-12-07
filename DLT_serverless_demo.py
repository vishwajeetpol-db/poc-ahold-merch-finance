# Databricks notebook source
import dlt
from pyspark.sql.functions import col

@dlt.table
def sales_customers():
    return (
        spark.table("samples.bakehouse.sales_customers")
        .select(
            col("customerID").cast("long"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("email_address").cast("string"),
            col("phone_number").cast("string"),
            col("address").cast("string"),
            col("city").cast("string"),
            col("state").cast("string"),
            col("country").cast("string"),
            col("continent").cast("string"),
            col("postal_zip_code").cast("long"),
            col("gender").cast("string")
        )
    )

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

@dlt.table(
    comment="Silver table for sales customers with expectations"
)
@dlt.expect_or_fail("valid_email", "email_address LIKE '%@%'")
@dlt.expect("valid_phone_number", "phone_number RLIKE '^[0-9]{10}$'")
@dlt.expect("valid_postal_code", "postal_zip_code IS NOT NULL")
def silver_sales_customers():
    return (
        dlt.read("sales_customers")
        .select(
            col("customerID"),
            col("first_name"),
            col("last_name"),
            col("email_address"),
            col("phone_number"),
            col("address"),
            col("city"),
            col("state"),
            col("country"),
            col("continent"),
            col("postal_zip_code"),
            col("gender")
        )
    )

# COMMAND ----------

import dlt
from pyspark.sql.functions import count, col

@dlt.table(
    comment="Gold table with aggregate data from silver_sales_customers"
)
def gold_sales_customers_aggregate():
    return (
        dlt.read("silver_sales_customers")
        .groupBy("country", "gender")
        .agg(
            count(col("customerID")).alias("customer_count")
        )
    )
