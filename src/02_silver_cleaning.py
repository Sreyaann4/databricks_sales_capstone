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
bronze_table = "workspace.default.capstone_bronze_sales"
silver_table = "workspace.default.capstone_silver_sales"

# Read Bronze table
df_bronze = spark.table(bronze_table)

print("Bronze records:", df_bronze.count())

# Add data quality flag
df_flagged = df_bronze.withColumn(
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

# Keep only valid business records
df_valid = df_flagged.filter(
    F.col("quality_flag") == "VALID"
)

print("Valid records:", df_valid.count())

# String columns to clean
string_columns = [
    "order_id",
    "customer_id",
    "customer_name",
    "city",
    "state",
    "product_id",
    "product_name",
    "category",
    "payment_method",
    "order_status"
]

# Remove leading and trailing spaces
df_clean = df_valid

for column_name in string_columns:
    df_clean = df_clean.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )

print("String trimming completed")

# Replace null values in descriptive columns
df_clean = df_clean.fillna(
    {
        "customer_name": "UNKNOWN",
        "city": "UNKNOWN",
        "state": "UNKNOWN",
        "product_name": "UNKNOWN",
        "category": "UNKNOWN",
        "payment_method": "UNKNOWN"
    }
)

print("Null handling completed")


# Explicit data type conversion
df_clean = (
    df_clean
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("discount_pct", F.col("discount_pct").cast("double"))
    .withColumn("gross_amount", F.col("gross_amount").cast("double"))
    .withColumn("discount_amount", F.col("discount_amount").cast("double"))
    .withColumn("net_amount", F.col("net_amount").cast("double"))
)

print("Data type conversion completed")
df_clean.printSchema()
#display(df_clean)

# Add date-based columns
df_silver = (
    df_clean
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("month_name", F.date_format("order_date", "MMMM"))
)

display(df_silver)


# Write cleaned valid data to Silver Delta table
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

print(f"Silver table created successfully: {silver_table}")

# Verify Silver table
df_silver_check = spark.table(silver_table)

print("Silver records:", df_silver_check.count())

#df_silver_check.printSchema()

display(df_silver_check)