import boto3
from dotenv import load_dotenv
import os

load_dotenv()

client = boto3.client("glue", region_name="ap-southeast-1")

DATABASE_NAME = "finnhub_pipeline"

# Initialize glue DATABASE_NAME

def create_database():
    try:
        client.create_database(
            DatabaseInput={"Name": DATABASE_NAME}
        )
        print(f"Created database: {DATABASE_NAME}")
    except client.exceptions.AlreadyExistsException:
        print(f"Database already exists: {DATABASE_NAME}")

# Table logic

tables = [
    {
        "name": "bronze_trades",
        "location": "s3://finnhub-pipeline-bronze/trades/",
        "columns": [
            {"Name": "s", "Type": "string"},
            {"Name": "p", "Type": "double"},
            {"Name": "v", "Type": "double"},
            {"Name": "t", "Type": "bigint"},
            {"Name": "c", "Type": "string"},
            {"Name": "ingested_at", "Type": "timestamp"},
        ]
    },
    {
        "name": "silver_trades",
        "location": "s3://finnhub-pipeline-silver/trades/",
        "columns": [
            {"Name": "symbol", "Type": "string"},
            {"Name": "price", "Type": "double"},
            {"Name": "volume", "Type": "double"},
            {"Name": "trade_timestamp_ms", "Type": "bigint"},
            {"Name": "trade_timestamp", "Type": "timestamp"},
            {"Name": "ingested_at", "Type": "timestamp"},
        ]
    },
    {
        "name": "gold_ohlcv",
        "location": "s3://finnhub-pipeline-gold/ohlcv/",
        "columns": [
            {"Name": "symbol", "Type": "string"},
            {"Name": "window_start", "Type": "timestamp"},
            {"Name": "window_end", "Type": "timestamp"},
            {"Name": "open", "Type": "double"},
            {"Name": "high", "Type": "double"},
            {"Name": "low", "Type": "double"},
            {"Name": "close", "Type": "double"},
            {"Name": "volume", "Type": "double"},
        ]
    }
]

def create_table(table):
    try:
        client.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput={
                "Name" : table["name"],
                "StorageDescriptor": {
                    "Columns" : table["columns"],
                    "Location" : table["location"],
                    "InputFormat" : "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat" : "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo" : {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                },
                "TableType" : "EXTERNAL_TABLE",
            }
        )
        print(f"Created table : {table['name']}")
    except client.exceptions.AlreadyExistsException:
        print(f"Table already exists : {table['name']}")

if __name__ == "__main__":
    create_database()
    for table in tables:
        create_table(table)
    print("Glue catalog setup complete.")

