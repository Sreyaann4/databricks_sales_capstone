# Databricks notebook source
from pyspark.sql import functions as F

# Path of source CSV stored in Databricks Volume
source_path = "/Volumes/workspace/ibm/v4/sales_source_1500.csv"

# Read the CSV file
df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

# Display the data
#display(df_bronze)
# Bronze table name
bronze_table = "workspace.default.capstone_bronze_sales"

# Write source data to Bronze Delta table
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table)
)

print(f"Bronze table created successfully: {bronze_table}")

df_bronze.printSchema()

# Verify Bronze table
df_check = spark.table("workspace.default.capstone_bronze_sales")

print("Total records:", df_check.count())

#display(df_check)