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
appName = "PySpark Structured Streaming with Kafka"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .config("spark.sql.shuffle.partitions", 4)\
    .getOrCreate()


df_stream = spark\
    .readStream\
    .format("kafka")\
    .options(**kafka_options)\
    .load()

df_stream.printSchema()

df_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# schema = StructType([ \
#         StructField("id", IntegerType()), \
#         StructField("shop_id", IntegerType()), \
#         StructField("app_id", IntegerType()), \
#         StructField("app_name", StringType()) \
#     ])

# data_df = df_stream.select(from_json(col("value").cast("string"), schema).alias("data")) \
#     .select("data.*")
# data_df.show()
# print(data_df)

# query = data_df.writeStream \
#     .outputMode("complete") \
#     .format("parquet") \
#     .option("path", "/path/to/dw/table/location") \
#     .option("checkpointLocation", "/path/to/checkpoint/location") \
#     .trigger(processingTime="1 minute") \
#     .start()

# query.awaitTermination()