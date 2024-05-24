from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, concat_ws
from pyspark.sql.types import TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Incremental ETL Pipeline") \
    .getOrCreate()

# Read data from S3 folder in Parquet format
s3_input_path = "s3://bucket/folder/"
df = spark.read.parquet(s3_input_path)

# Transformation: Correct datetime format
df = df.withColumn("datetime", expr("to_timestamp(datetime, 'yyyy-MM-dd HH:mm:ss')"))

# Transformation: Filter by city
df = df.filter(col("city") == "New York")

# Transformation: Create a new column for full name
df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

# Transformation: Create a new column for matching gender (M for male, F for female)
df = df.withColumn("gender_match", when(col("gender") == "M", "M").otherwise("F"))

# Transformation: Map customer ID to state address from another Parquet file
# Assuming 'customer_id' is the common key between the two Parquet files
state_address_df = spark.read.parquet("s3://your-state-address-bucket/state_address.parquet")
df = df.join(state_address_df, "customer_id", "left")


# Partition by date
df = df.withColumn("date", expr("date(datetime)"))

# Define the output S3 path
s3_output_path = "s3://your-output-bucket/your-output-folder/"

# Write the transformed DataFrame to S3 in Parquet format partitioned by date and city
df.write.partitionBy("date", "city").mode("overwrite").parquet(s3_output_path)

# Stop Spark session
spark.stop()
