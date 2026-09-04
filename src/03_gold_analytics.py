# Databricks notebook source

from pyspark.sql import functions as F

source_path = "/Volumes/workspace/ibm/v4/sales_source_1500.csv"

df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

# Table names
silver_table = "workspace.default.capstone_silver_sales"
gold_table = "workspace.default.capstone_gold_sales_summary"

# Read cleaned Silver data
df_silver = spark.table(silver_table)

df_gold = (
    df_silver
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("gross_amount"), 2).alias("gross_sales"),
        F.round(F.sum("discount_amount"), 2).alias("total_discount_amount"),
        F.round(F.sum("net_amount"), 2).alias("net_sales"),
        F.round(
            F.sum("net_amount") / F.countDistinct("order_id"),
            2
        ).alias("average_order_value")
    )
)

#display(df_gold)

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

print(f"Gold table created successfully: {gold_table}") 


#for dashboard
