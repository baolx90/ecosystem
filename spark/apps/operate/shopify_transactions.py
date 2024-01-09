from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import happybase

import time

kafka_options = {
    "kafka.bootstrap.servers": "192.168.1.96:9092",
    "subscribe": "operate.public.shopify_transactions",
    "startingOffsets": "earliest"
}
HBASE_URL = '192.168.1.96'
#HBASE_URL = 'localhost'
TABLE_NAME = 'shopify_transactions'
hconn = happybase.Connection(HBASE_URL, autoconnect=False)

SCHEMA_DATA = StructType([
    StructField("id", StringType()),
    StructField("shop_id", StringType()),
    StructField("app_id", StringType()),
    StructField("app_name", StringType()),
    StructField("shopify_domain", StringType()),
    StructField("event", StringType()),
    StructField("charge_id", StringType()),
    StructField("charge_amount", DoubleType()),
    StructField("charge_created_at", StringType()),
    StructField("billing_interval", StringType()),
    StructField("cursor", StringType()),
    StructField("gross_amount", DoubleType()),
    StructField("net_amount", DoubleType()),
    StructField("shopify_fee", DoubleType()),
    StructField("processing_fee", DoubleType()),
    StructField("regulatory_operating_fee", DoubleType())
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
	                'info:shopify_domain': (collect.shopify_domain),
	                'info:event': (collect.event),
	                'info:charge_id': (collect.charge_id),
	                'info:charge_amount': (collect.charge_amount),
	                'info:charge_created_at': (collect.charge_created_at),
	                'info:billing_interval': (collect.billing_interval),
	                'info:cursor': (collect.cursor),
	                'info:gross_amount': (collect.gross_amount),
	                'info:net_amount': (collect.net_amount),
	                'info:shopify_fee': (collect.shopify_fee),
	                'info:processing_fee': (collect.processing_fee),
	                'info:regulatory_operating_fee': (collect.regulatory_operating_fee),
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

    payload_df = payload_df.withColumn('charge_created_at', from_unixtime(payload_df.charge_created_at/1000, "yyyy-MM-dd HH:mm:ss"))

    query = payload_df.writeStream\
        .format("console") \
        .outputMode("append")\
        .foreachBatch(persist_to_hbase)\
        .start()\
        .awaitTermination()