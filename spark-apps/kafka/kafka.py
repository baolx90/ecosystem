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

def parse_data_from_kafka_message(sdf, schema):
    # sdf = sdf.select(json_tuple(col("value"),"payload")) \
    #     .toDF("payload")
    # print(sdf)
    # print(sdf['value'])
    # # sdf['value'].collect().show()
    col = split(sdf['value'], ',')
    print(col)
    for idx, field in enumerate(schema): 
        print(idx, field)
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

def write_to_postgres(df, epoch_id):
        mode="append"
        url = "jdbc:postgresql://192.168.1.54:5432/kafka"
        properties = {"user": "postgres", "password": "x9huFSwx8ESC3vg3", "driver": "org.postgresql.Driver"}
        df.write.jdbc(url=url, table="bao", mode=mode, properties=properties)

if __name__ == "__main__":
    print("Stream Data Processing Starting ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
        .config("spark.sql.shuffle.partitions", 4)\
        .getOrCreate()

    df = spark\
        .read\
        .format("kafka")\
        .options(**kafka_options)\
        .load()

    schema = StructType([ \
        StructField("id", IntegerType()), \
        StructField("shop_id", IntegerType()), \
        StructField("app_id", IntegerType()), \
        StructField("app_name", StringType()) \
    ])

    # json_df = df.selectExpr("CAST(value AS STRING) as event_value").select(from_json("event_value",schema).alias("value"))
    # json_df.writeStream \
    #     .foreachBatch(write_to_postgres) \
    #     .option("checkpointLocation", '/checkpoint_path') \
    #     .outputMode('update') \
    #     .start() \
    #     .awaitTermination()
    # json_df.show(truncate=False)
    # json_df = df.selectExpr("CAST(value AS STRING) as value")

    # json_df.show(truncate=False)

    payload_df = df.selectExpr("CAST(value AS STRING) as value").alias("value")\
        .select(json_tuple(col("value"),"payload")).toDF("payload")

    # a = payload_df.withColumn("payload", to_json(col("payload")))
    # a.show(truncate=False)
    payload_df.show(truncate=False)
    # json_df = df.selectExpr("CAST(value AS STRING) as value")
    # for item in payload_df.collect():
    #     print(item)
    # payload_df.show(truncate=False)

    # payload_df
    spark.stop()


    # # Implementing the JSON functions in Databricks in PySpark
    # spark = SparkSession.builder.appName('PySpark JSON').getOrCreate()
    # Sample_Json_String = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    # dataframe = spark.createDataFrame([(1, Sample_Json_String)],["id","value"])
    # dataframe.show(truncate=False)
    # # Using from_json() function
    # dataframe2 = dataframe.withColumn("value", from_json(dataframe.value,MapType(StringType(), StringType())))
    # dataframe2.printSchema()
    # dataframe2.show(truncate=False)
    # # Using to_json() function
    # dataframe2.withColumn("value", to_json(col("value"))) \
    # .show(truncate=False)
    # # Using json_tuple() function
    # dataframe.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
    # .toDF("id","Zipcode","ZipCodeType","City") \
    # .show(truncate=False)
    # # Using get_json_object() function
    # dataframe.select(col("id"), get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
    # .show(truncate=False)
    # # Using schema_of_json() function
    # Schema_Str = spark.range(1) \
    # .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
    # .collect()[0][0]
    # print(Schema_Str)


    # dataframe2 = json_df.withColumn("value", from_json(json_df.value,MapType(StringType(), StringType())))
    # dataframe2.printSchema()
    # dataframe2.show(truncate=False)

    # dataframe2.withColumn("value", to_json(col("value"))).show(truncate=False)
    
    
    # schema = StructType([ \
    #     StructField("id", IntegerType()), \
    #     StructField("shop_id", IntegerType()), \
    #     StructField("app_id", IntegerType()), \
    #     StructField("app_name", StringType()) \
    # ])
    # json_expanded_df = df.withColumn("value", from_json(df.value, MapType(StringType(), StringType())))
    # # payload = parse_data_from_kafka_message(df, StructType([ \
    # #     StructField("payload", StringType()) \
    # # ]))
    # json_expanded_df.show(truncate=False)
    # json_expanded_df.printSchema()

    # json_df = df.selectExpr("CAST(value AS STRING) as value")
    # json_df.show(truncate=False)
    # print(json_df["value"]) 
    # json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], schema)).select("value.*")
    # json_expanded_df.show(truncate=False)
    # print(json_expanded_df)
    # json_expanded_df.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()


    # .option("checkpointLocation", "path/to/test/dir") \
    # df = df.selectExpr("CAST(value AS STRING)")

    # df = df.select(json_tuple(col("value"),"payload")) \
    #     .toDF("payload")

    # df = df.select(json_tuple(col("payload"),"before","after","source","op")) \
    #     .toDF("before","after","source","op")
    # # df.show()
    # df.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()

    # df_stream = spark\
    #     .readStream\
    #     .format("kafka")\
    #     .options(**kafka_options)\
    #     .load()\
    #     .selectExpr("CAST(value AS STRING)")

    # # print(df_stream)
    # # query = df_stream \
    # #     .writeStream \
    # #     .format("console") \
    # #     .start()
    # # query.awaitTermination()

    # schema = StructType([ \
    #     StructField("id", IntegerType()), \
    #     StructField("shop_id", IntegerType()), \
    #     StructField("app_id", IntegerType()), \
    #     StructField("app_name", StringType()) \
    # ])
    # df.select(
    #     # Convert the value to a string
    #     from_json(
    #         decode(col("value"), "iso-8859-1"),
    #         schema
    #     ).alias("value")
    # )\
    # .select("value.*")\
    # .writeStream\
    # .outputMode("append")\
    # .format("console")\
    # .start()\
    # .awaitTermination()
    # personDF = df_stream.select(from_json(col("value"), schema)).alias("data")
    # personDF.printSchema()

    # personDF.writeStream\
    #   .format("console")\
    #   .outputMode("append")\
    #   .start()\
    #   .awaitTermination()
        
    # value_test = parse_data_from_kafka_message(df, schema)

    # value_test.writeStream\
    #   .format("console")\
    #   .outputMode("append")\
    #   .start()\
    #   .awaitTermination()
    # value_test.printSchema()
    # value_test.show()
    # print(value_test)

    # value_test.writeStream \
    #     .outputMode("append") \
    #     .format("parquet") \
    #     .option("path", "data/BaoData") \
    #     .option("checkpointLocation", "data/BaoData") \
    #     .option("truncate", False) \
    #     .start()\
    #     .awaitTermination()


    # df_stream = df_stream.select(json_tuple(col("value"),"payload")) \
    #     .toDF("payload")

    # df_stream = df_stream.select(json_tuple(col("payload"),"before","after","source","op")) \
    #     .toDF("before","after","source","op")

    # df_stream.writeStream\
    #   .format("console")\
    #   .outputMode("append")\
    #   .start()\
    #   .awaitTermination()
    # print(df_stream)
    # for line in df_stream.collect():
    #     print(line)
    #     op = line.__getitem__('op')
    #     print(op)




    # print(df_stream_value[0]) 
    # print(df_stream_value[0].__getitem__('after')) 
    # a = df_stream_value
    # print(a)

    # df_stream_value.show(1)
    
    # payload_df = df_stream_value.withColumn("value",from_json(df_stream_value.value,MapType(StringType(),StringType())))

    # payload_df = df_stream_value\
    #     .select(get_json_object(df_stream_value.value, "$.payload").alias("payload"))\
    # payload_df.printSchema()
    # payload_df.show(1)
    # for k in payload_df.collect():
    #     print(k.payload)
    #     print(spark.read.json(k.payload))

    # after_df = payload_df.select(get_json_object(payload_df.payload, "$.after").alias("after"))
    # after_df.printSchema()
    # after_df.show(truncate=False)

    # data = after_df.select(json_tuple(col("after"),"id","shop_id")).toDF("id","shop_id")
    # data.printSchema()
    # data.show(truncate=False)
    # for k in data.collect():
    #     print(k)

    # df1 = payload_df.select(json_tuple(col("payload"),"after","op"))
    #     .toDF("after","op")
    # df1.printSchema()
    # print("Printing payload_df: ")
    # payload_df.printSchema()
    # payload_df.show(10)

    # data = payload_df.select(json_tuple(col("payload"),"after","op"))
    # .toDF("after","op")
    # for row in data:
    #     print(row.after,row.op)
    # a.foreach(data)
    # a.where(a.op == "r").foreach(create)
    # create(a.where(a.op == "r"))
    # create(a.where(a.op == "u"))

    # b = a.where(a.op == "u").show(truncate=False)

    # schema = StructType() \
    #     .add("id",IntegerType()) \
    #     .add("shop_id",IntegerType())


    # df = df_time.select(F.from_json(F.col("value"), orders_schema).alias("order"), "timestamp")
##
## TODO TO UPDATE kafka.sasl.jaas.config
##


# Subscribe to Kafka topic "hello"


# Validate Schema
# streaming_df.printSchema()
# streaming_df.show(truncate=False)

# json_schema = StructType([
#     StructField('customerId', StringType(), True), \
#     StructField('data', StructType([
#         StructField('devices', ArrayType(StructType([ \
#         StructField('deviceId', StringType(), True), \
#         StructField('measure', StringType(), True), \
#         StructField('status', StringType(), True), \
#         StructField('temperature', LongType(), True)
#     ]), True), True)]), True), \
#     StructField('eventId', StringType(), True), \
#     StructField('eventOffset', LongType(), True), \
#     StructField('eventPublisher', StringType(), True), \
#     StructField('eventTime', StringType(), True)
# ])

# json_schema = StructType([
#     StructField("id", IntegerType()),
#     StructField("shop_id", IntegerType())
# ])

# json_df = streaming_df.selectExpr("cast(value as string) as value")
# json_df.show(10, False)

# json_expanded_df = json_df.withColumn("payload", from_json(json_df["payload"], json_schema)).select("value.*")

# # Validate Schema
# json_expanded_df.show(10, False)
# json_expanded_df.printSchema()

# # exploded_df = json_expanded_df \
# #     .select("id", "shop_id")

# # exploded_df.printSchema()
# # exploded_df.show(truncate=False)


# full_schema = StructType([
#     StructField("schema", StructType([
#         StructField("type", StringType(), False),
#         StructField("name", StringType(), False),
#         StructField("fields", StructType([
#             StructField("field", StringType(), False),
#             StructField("type", StringType(), False)
#         ]))
#     ])),
#     StructField("payload", StructType([
#         StructField("id", IntegerType()),
#         StructField("shop_id", IntegerType())
#     ]))
# ])
# schema = StructField("payload", StructType([
#         StructField("id", IntegerType()),
#         StructField("shop_id", IntegerType())
#     ]))

# kafka_df = streaming_df.selectExpr("CAST(value AS STRING)")
# kafka_df.show(truncate=False)

# def f(kafka_df):
#     print(get_json_object(kafka_df.value, "$.payload"))

# kafka_df.foreach(f)

# payload_df = kafka_df.select(get_json_object(kafka_df.value, "$.payload").alias("payload"))
# before_df = kafka_df.select(get_json_object(kafka_df.value, "$.payload.before").alias("before"))
# after_df = kafka_df.select(get_json_object(kafka_df.value, "$.payload.after").alias("after"))
# payload_df.show()
# before_df.show()
# after_df.show()

# emp_df = payload_df.select(from_json(col('payload'), schema).alias("DF")).select("DF.*")
# emp_df.show()

# df = kafka_df.select(get_json_object(kafka_df.value, "$.payload").alias("payload"))
# final_df = df.rdd.map(lambda row: json.loads(row[0])).toDF().show()


# kafka_df.show(truncate=False)
# kafka_df.select(get_json_object(kafka_df['value'],"$.payload").alias('payload'))\
#     .select(from_json(col('payload'), full_schema).alias("DF"))\
#     .select("DF.*")\
#     .show(truncate=False)


# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# df.printSchema()


# json_df = df.selectExpr("cast(value as string) as value")
# json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

# writing_df = json_expanded_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()
# writing_df.awaitTermination()


# sample_schema = StructType()\
#         .add("id", StringType())\
#         .add("shop_id", StringType())
# info_dataframe = df.select(from_json(col("value"), sample_schema).alias("sample"))
# info_df_fin = info_dataframe.select("sample.*")
# info_df_fin.show()


# words = df.select(
#    explode(
#        split(df.value, " ")
#    ).alias("word")
# )
# print(words)
# print(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))

# # query = df\
# #     .selectExpr("CAST(value AS STRING)")\
# #     .outputMode("append") \
# #     .format("console") \
# #     .start()
# query = df\
#     .select(col("value"))\
#     .writeStream\
#     .format("console")\
#     .start()
# query.awaitTermination()

# # Deserialize the value from Kafka as a String for now
# deserialized_df = df.selectExpr("CAST(value AS STRING)")

# # Query Kafka and wait 10sec before stopping pyspark
# query = deserialized_df.writeStream.outputMode("append").format("console").start()
# time.sleep(10)
# query.stop()






# orders_df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", kafka_topic_name) \
#     .option("startingOffsets", "latest") \
#     .load()

# orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")


# stock_price_schema = types.StructType([
#     types.StructField("symbol", types.StringType(), True),
#     types.StructField("date", types.DateType(), True),
#     types.StructField("open", types.DoubleType(), True),
#     types.StructField("high", types.DoubleType(), True),
#     types.StructField("low", types.DoubleType(), True),
#     types.StructField("close", types.DoubleType(), True),
#     types.StructField("volume", types.IntegerType(), True)
# ])

# orders_df2 = orders_df1\
#     .select(from_json(col("value"), stock_price_schema)\
#     .alias("orders"), "timestamp")
    
# # orders_df3 = orders_df2.select("orders.*", "timestamp")

# stock_df = orders_df2.select("orders.*")
    
# # Write final result into console for debugging purpose
# orders_agg_write_stream = stock_df \
#     .writeStream \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("update") \
#     .option("truncate", "false")\
#     .format("console") \
#     .start()


# def update(stock_df):
    
#     if stock_df.count() == 0:
#         return
    
#     for row in stock_df.rdd.collect():
        
#         symbol = row["symbol"]
#         df_new = stock_df[stock_df["symbol"] == symbol]
#         df_old = spark.read.parquet(f"data/pq/{symbol}/")
        
#         df_new = df_old.union(df_new).distinct()
#         df_new.repartition(4).write.parquet(f"data/pq/{symbol}/")

# update(orders_agg_write_stream)
# orders_agg_write_stream.awaitTermination()
