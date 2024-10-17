from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

# Step 1: Initialize Spark session
spark = SparkSession.builder \
    .appName("Ecommerce Data Analysis") \
    .getOrCreate()

# Step 2: Load data
df = spark.read.csv('eccomerce.csv', header=True, inferSchema=True)

# Step 3: Handle missing values
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Step 4: Data processing - Filter and aggregate
df_filtered = df.filter(col("Year") == 2023)
df_filtered.groupBy("Month").sum("grand_total").show()

# Step 5: Save processed data
df_filtered.write.csv("filtered_data.csv", header=True)

# Step 6: Stop Spark session
spark.stop()
