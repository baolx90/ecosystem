from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import happybase

import time

kafka_options = {
    "kafka.bootstrap.servers": "192.168.1.96:9092",
    "subscribe": "operate.public.shopify_histories",
    "startingOffsets": "earliest"
}
HBASE_URL = '192.168.1.96'
#HBASE_URL = 'localhost'
TABLE_NAME = 'shopify_histories'
hconn = happybase.Connection(HBASE_URL, autoconnect=False)

SCHEMA_DATA = StructType([
    StructField("id", StringType()),
    StructField("shop_id", StringType()),
    StructField("app_id", StringType()),
    StructField("app_name", StringType()),
    StructField("shopify_domain", StringType()),
    StructField("event", StringType()),
    StructField("detail", StringType()),
    StructField("cursor", StringType()),
    StructField("occurred_at", StringType()),
    StructField("subscription_id", StringType()),
    StructField("created_at", StringType()),
    StructField("billing_on", StringType()),
    StructField("app_credit_id", StringType()),
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
        if collect.op == "d":
            table.delete(collect.id)
        else:
            table.put(
                (collect.id), 
                {
                    'info:id': (collect.id),
                    'info:shop_id': (collect.shop_id),
                    'info:app_id': (collect.app_id),
                    'info:app_name': (collect.app_name),
                    'info:shopify_domain': (collect.shopify_domain),
                    'info:event': (collect.event),
                    'info:detail': (collect.detail),
                    'info:cursor': (collect.cursor),
                    'info:occurred_at': (collect.occurred_at),
                    'info:subscription_id': (collect.subscription_id),
                    'info:created_at': (collect.created_at),
                    'info:billing_on': (collect.billing_on),
                    'info:app_credit_id': (collect.app_credit_id),
                }
            )
    
    hconn.close()

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Operate Streaming Shopify Transactions") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
        .config("spark.sql.shuffle.partitions", 4)\
        .enableHiveSupport() \
        .getOrCreate()

    df = spark\
        .readStream\
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

    payload_df = payload_df.withColumn('occurred_at', from_unixtime(payload_df.occurred_at/1000, "yyyy-MM-dd HH:mm:ss"))\
        .withColumn('created_at', from_unixtime(payload_df.created_at/1000, "yyyy-MM-dd HH:mm:ss"))\
        .withColumn('billing_on', from_unixtime(payload_df.billing_on/1000, "yyyy-MM-dd HH:mm:ss"))

    query = payload_df.writeStream\
        .format("console") \
        .outputMode("append")\
        .foreachBatch(persist_to_hbase)\
        .start()\
        .awaitTermination()