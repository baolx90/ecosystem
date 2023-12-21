from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

import time

kafka_options = {
    "kafka.bootstrap.servers": "baolx.local:9092",
    "subscribe": "operate.public.shopify_app_credits",
    "startingOffsets": "earliest"
}

print("Stream Data Processing Starting ...")
print(time.strftime("%Y-%m-%d %H:%M:%S"))

spark = SparkSession \
    .builder \
    .appName("PySpark Structured Streaming with Kafka Demo") \
    .config("spark.sql.shuffle.partitions", 4)\
    .getOrCreate()

df = spark\
    .read\
    .format("kafka")\
    .options(**kafka_options)\
    .load()
df.printSchema()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
# .writeStream \
# .format("console") \
# .outputMode("append") \
# .option("checkpointLocation", "path/to/test/dir") \
# .start() \
# .awaitTermination()