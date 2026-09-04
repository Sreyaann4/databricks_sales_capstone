# Databricks notebook source

from pyspark.sql import functions as F

source_path = "/Volumes/workspace/ibm/v4/sales_source_1500.csv"

df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

# Bronze table
bronze_table = "workspace.default.capstone_bronze_sales"

# Read Bronze data
df = spark.table(bronze_table)

# Create data quality flag
df_quality = df.withColumn(
    "quality_flag",
    F.when(
        F.col("customer_id").isNull(),
        "INVALID_CUSTOMER"
    )
    .when(
        F.col("quantity") <= 0,
        "INVALID_QUANTITY"
    )
    .when(
        F.col("net_amount") < 0,
        "INVALID_AMOUNT"
    )
    .when(
        F.col("product_id") == "UNKNOWN",
        "INVALID_PRODUCT"
    )
    .otherwise("VALID")
)

# Display records with their quality flag
display(df_quality)


# Count records by quality flag
quality_summary = (
    df_quality
    .groupBy("quality_flag")
    .count()
    .orderBy("quality_flag")
)

display(quality_summary)


# Filter invalid records
invalid_records = df_quality.filter(
    F.col("quality_flag") != "VALID"
)

print("Total invalid records:", invalid_records.count())

display(invalid_records)
# Data quality summary
quality_summary = (
    df_quality
    .groupBy("quality_flag")
    .agg(
        F.count("*").alias("record_count")
    )
    .orderBy("quality_flag")
)

display(quality_summary)


# Overall data quality metrics
total_records = df_quality.count()

valid_records = df_quality.filter(
    F.col("quality_flag") == "VALID"
).count()

invalid_count = total_records - valid_records

quality_metrics = spark.createDataFrame(
    [
        (
            total_records,
            valid_records,
            invalid_count,
            round((valid_records / total_records) * 100, 2)
        )
    ],
    [
        "total_records",
        "valid_records",
        "invalid_records",
        "valid_percentage"
    ]
)

display(quality_metrics)

