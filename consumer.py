from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, trim

# Create Spark Session
spark = SparkSession.builder \
    .appName("ApplicationLogConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Subscribe to Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "applog") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka key and value into strings
log_df = kafka_df.select(
    col("key").cast("string").alias("LogLevel"),
    col("value").cast("string").alias("LogMessage")
)

# Count words in each message
result_df = log_df.withColumn(
    "WordCount",
    size(split(trim(col("LogMessage")), r"\s+"))
)

# Display results on console
query = result_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option(
        "checkpointLocation",
        "file:///home/takeo/bootcamp/kafka-log-project/checkpoints"
    ) \
    .start()

query.awaitTermination()