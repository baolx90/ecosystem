from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import happybase

import time

kafka_options = {
    "kafka.bootstrap.servers": "192.168.1.96:9092",
    "subscribe": "operate.public.shopify_app_credits",
    "startingOffsets": "earliest"
}
#HBASE_URL = '192.168.1.96'
HBASE_URL = 'localhost'
TABLE_NAME = 'shopify_app_credits'
hconn = happybase.Connection(HBASE_URL, autoconnect=False)

SCHEMA_DATA = StructType([
    StructField("id", StringType()),
    StructField("shop_id", StringType()),
    StructField("app_id", StringType()),
    StructField("app_name", StringType()),
    StructField("name", StringType()),
    StructField("price", StringType()),
])

def create_table(hconn, table_name):
    try:
        return hconn.create_table(
            table_name,
            {
                'info': dict()
            })
    except Exception as error:
        return hconn.table(table_name)
    

def persist_to_hbase(batch_df, batch_id):
    hconn.open()
    table = create_table(hconn, TABLE_NAME)

    for collect in batch_df.collect():
        print(collect)
        if collect.op == "d" :
            table.delete(collect.id)
        else :
            table.put(
                (collect.id), 
                {
                    'info:id': (collect.id),
                    'info:shop_id': (collect.shop_id),
                    'info:app_id': (collect.app_id),
                    'info:app_name': (collect.app_name),
                    'info:name': (collect.name),
                    'info:price': collect.price
                }
            )
    
    hconn.close()

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Operate Streaming Shopify App Credits") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
        .config("spark.sql.shuffle.partitions", 4)\
        .enableHiveSupport() \
        .getOrCreate()

    df = spark\
        .read\
        .format("kafka")\
        .options(**kafka_options)\
        .load()

    payload_df = df.selectExpr("CAST(value AS STRING) as value")\
        .alias("value")\
        .select(json_tuple(col("value"),"payload"))\
        .toDF("payload")\
        .select(json_tuple(col("payload"),"before","after","source","op")) \
        .toDF("before","after","source","op")\
        .selectExpr("CAST(after AS STRING) as after", "op")\
        .withColumn("after",from_json(col("after"),SCHEMA_DATA))\
        .select("after.*","op")

    persist_to_hbase(payload_df, 123)

    # query = payload_df.writeStream\
    #     .format("console") \
    #     .outputMode("append")\
    #     .foreachBatch(persist_to_hbase)\
    #     .start()\
    #     .awaitTermination()