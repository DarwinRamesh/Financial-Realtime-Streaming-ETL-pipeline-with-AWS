import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BRONZE_PATH = "s3a://finnhub-pipeline-bronze/trades/"
S3_SILVER_PATH = "s3a://finnhub-pipeline-silver/trades/"

# Spark init 

spark = SparkSession.builder \
        .appName("SilverStream")\
        .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_schema = StructType([
    StructField("s", StringType()),
    StructField("p", DoubleType()),
    StructField("v", DoubleType()),
    StructField("t", LongType()),
    StructField("c", StringType()),
    StructField("ingested_at", TimestampType()),
])

bronze_df = spark.readStream \
    .format("parquet") \
    .option("path", S3_BRONZE_PATH) \
    .schema(bronze_schema) \
    .load()

# Silver inti

silver_df = bronze_df \
    .filter(col("s").isNotNull()) \
    .filter(col("p") > 0) \
    .filter(col("v") > 0) \
    .withColumnRenamed("s", "symbol") \
    .withColumnRenamed("p", "price") \
    .withColumnRenamed("v", "volume") \
    .withColumnRenamed("t", "trade_timestamp_ms") \
    .withColumn("trade_timestamp",
                to_timestamp(from_unixtime(col("trade_timestamp_ms") / 1000))) \
    .drop("c") \
    .dropDuplicates(["symbol", "trade_timestamp_ms"])

query = silver_df.writeStream \
    .format("parquet") \
    .option("path", S3_SILVER_PATH) \
    .option("checkpointLocation", "s3a://finnhub-pipeline-silver/checkpoints/silver/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()

