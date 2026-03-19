import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, first, max, min, last, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from dotenv import load_dotenv

load_dotenv()


AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_SILVER_PATH = "s3a://finnhub-pipeline-silver/trades/"
S3_GOLD_PATH = "s3a://finnhub-pipeline-gold/ohlcv/"

spark = SparkSession.builder \
    .appName("GoldStream") \
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

silver_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("trade_timestamp_ms", LongType()),
    StructField("trade_timestamp", TimestampType()),
    StructField("ingested_at", TimestampType()),
])

silver_df = spark.readStream \
    .format("parquet") \
    .schema(silver_schema) \
    .option("path", S3_SILVER_PATH) \
    .load()

ohlcv_df = silver_df \
    .withWatermark("trade_timestamp", "2 minutes") \
    .groupBy(
        window(col("trade_timestamp"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("volume").alias("volume")
    ) \
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume")
    )

query = ohlcv_df.writeStream \
    .format("parquet") \
    .option("path", S3_GOLD_PATH) \
    .option("checkpointLocation", "s3a://finnhub-pipeline-gold/checkpoints/gold/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
