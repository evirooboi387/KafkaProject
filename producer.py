from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace
from pathlib import Path
import os

# Create Spark Session
spark = SparkSession.builder \
    .appName("ApplicationLogProducer") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read application.log
file_path = Path(__file__).resolve().with_name(
    "application.log"
).as_uri()
df = spark.read.text(file_path)

# Split log level and log message
log_df = df.filter(col("value").contains(":")) \
    .select(
        regexp_replace(
            split(col("value"), ":", 2).getItem(0),
            r"(?U)^\s+|\s+$", ""
        ).alias("LogLevel"),

        regexp_replace(
            split(col("value"), ":", 2).getItem(1),
            r"(?U)^\s+|\s+$", ""
        ).alias("LogMessage")
    )

# Display the DataFrame
log_df.show(truncate=False)

# Convert columns into Kafka key and value
kafka_df = log_df.select(
    col("LogLevel").cast("string").alias("key"),
    col("LogMessage").cast("string").alias("value")
)

# Publish data to Kafka
kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "applog") \
    .option("kafka.acks", "all") \
    .save()

print("Application logs successfully published to Kafka.")

spark.stop()