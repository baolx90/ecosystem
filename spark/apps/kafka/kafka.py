from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from pyspark.sql.functions import *
import happybase

import time

kafka_options = {
    "kafka.bootstrap.servers": "192.168.1.96:9092",
    "subscribe": "operate.public.shopify_transactions",
    "startingOffsets": "earliest"
}

def create_table(connection, table_name):
    try:
        connection.create_table(
            table_name,
            {
                'info': dict()
            })
    except grpcface.NetworkError as exp:
        if exp.code != interfaces.StatusCode.ALREADY_EXISTS:
            raise
        print("Table already exists.")
    return connection.table(table_name)

def persist_to_hbase(batch_df, batch_id):
    connection = happybase.Connection('192.168.1.96')
    # TABLE_NAME = 'shopify_histories'
    # table = create_table(connection, TABLE_NAME)
    table = connection.table('shopify_histories')

    for collect in batch_df.collect():
        table.put(
            (collect.id), 
            {
                b'info:id': (collect.id),
                b'info:shop_id': (collect.shop_id),
                b'info:app_id': (collect.app_id),
                b'info:app_name': (collect.app_name),
                b'info:shopify_domain': (collect.shopify_domain),
                b'info:event': (collect.event),
                b'info:detail': (collect.detail),
                b'info:cursor': (collect.cursor),
                b'info:occurred_at': (collect.occurred_at),
                b'info:subscription_id': (collect.subscription_id),
                b'info:created_at': (collect.created_at),
                b'info:billing_on': (collect.billing_on),
                b'info:app_credit_id': (collect.app_credit_id),
            }
        )
    

    connection.close()

if __name__ == "__main__":
    print("Stream Data Processing Starting ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
        .config("spark.sql.shuffle.partitions", 4)\
        .enableHiveSupport() \
        .getOrCreate()

    df = spark\
        .readStream\
        .format("kafka")\
        .options(**kafka_options)\
        .load()


    # df1 = df.select(explode(split(df.data, '\n')))
    # df1.printSchema()
    # df1.show(truncate=False)
    # Write data to employee_data and store checkpoint

    # query = df1.writeStream.outputMode("append")\
    #     .option("checkpointLocation","/Users/arvin/checkpoint")\
    #     .option("path","hdfs://master:9000/usr/hive/warehouse/test_data") \
    #     .start()
    # query.awaitTermination()
    schema = StructType([
        StructField("id", StringType()),
        StructField("shop_id", StringType()),
        StructField("app_id", StringType()),
        StructField("app_name", StringType()),
        StructField("shopify_domain", StringType()),
        StructField("event", StringType()),
        StructField("detail", StringType()),
        StructField("cursor", StringType()),
        StructField("occurred_at", TimestampType()),
        StructField("subscription_id", StringType()),
        StructField("created_at", TimestampType()),
        StructField("billing_on", TimestampType()),
        StructField("app_credit_id", StringType()),
    ])

    # query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .write \
    #     .format("console") \
    #     .start()


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
    # payload_df = df.selectExpr("CAST(value AS STRING) as value")\
    #     .alias("value")\
    #     .select(json_tuple(col("value"),"payload"))\
    #     .toDF("payload")\
    #     .select(json_tuple(col("payload"),"before","after","source","op")) \
    #     .toDF("before","after","source","op")\
    #     .selectExpr("CAST(after AS STRING) as after")\
    #     .alias("after")
    #     .withColumn('id', after.getItem(idx).cast(field.dataType))
    # value_test = parse_data_from_kafka_message(payload_df, schema)

    # value_test.show(truncate=False)
    # ok
    payload_df = df.selectExpr("CAST(value AS STRING) as value")\
        .alias("value")\
        .select(json_tuple(col("value"),"payload"))\
        .toDF("payload")\
        .select(json_tuple(col("payload"),"before","after","source","op")) \
        .toDF("before","after","source","op")\
        .selectExpr("CAST(after AS STRING) as after")\
        .alias("after")\
        .withColumn("after",from_json(col("after"),schema))\
        .select("after.*")

    query = payload_df.writeStream\
        .format("console") \
        .outputMode("append")\
        .foreachBatch(persist_to_hbase)\
        .start()\
        .awaitTermination()


    # for k in payload_df.collect():
    #     print(k.id)
    #     a = spark.read.json(k.after)
    #     print(a)

    # query = payload_df.writeStream\
    #     .foreachBatch(foreach_batch_function)\
    #     .start()
    # payload_df.printSchema()
    # payload_df.show(truncate=False)
    # for k in payload_df.collect():
    #     print(payload_df.after)
    #     a = spark.read.json(k.after)
    #     print(a)

    # payload_df = df.selectExpr("CAST(value AS STRING) as value")\
    #     .alias("value")\
    #     .select(json_tuple(col("value"),"payload"))\
    #     .toDF("payload")\
    #     .select(json_tuple(col("payload"),"before","after","source","op")) \
    #     .toDF("before","after","source","op")\
    #     .selectExpr("CAST(after AS STRING) as after")\
    #     .alias("after")\
    #     .withColumn("after",from_json(col("after"),schema))\
    #     .select("after.*")

    # # value_test = parse_data_from_kafka_message(payload_df, schema)
    # # df2 = payload_df.withColumn("after",from_json(payload_df.after,schema)).select("after.*")
    # payload_df.printSchema()
    # payload_df.show(truncate=False)
    # payload_df.write.mode("overwrite").saveAsTable("test_table")

    # ok

    # # value_test = parse_data_from_kafka_message(payload_df, schema)
    # df2 = payload_df.withColumn("after",from_json(payload_df.after,MapType(StringType(),StringType())))
    # # df2.printSchema()
    # df2.show(truncate=False)

    # value_test = df2.rdd.map(lambda x: x.after)
    # # df3=df2.rdd.map(lambda x: \
    # #     (\
    # #         int(x.after["id"]),\
    # #         int(x.after["shop_id"]),\
    # #         int(x.after["app_id"]),\
    # #         x.after["app_name"]\
    # #     )\
    # # )
    # # value_test.printSchema()
    # print(value_test)
    # # value_test.show()


    # payload_df = df.selectExpr("CAST(value AS STRING) as value")\
    #     .alias("value")\
    #     .select(json_tuple(col("value"),"payload"))\
    #     .toDF("payload")\
    #     .select(json_tuple(col("payload"),"before","after","source","op")) \
    #     .toDF("before","after","source","op")\
    #     .selectExpr("CAST(after AS STRING) as after")\
    #     .alias("after")
    # df2 = payload_df.withColumn("after",from_json(payload_df.after,MapType(StringType(),StringType())))
    # df2.printSchema()
    # df2.show(truncate=False)

    # df3=df2.rdd.map(lambda x: \
    #     (x.after["id"],x.after["shop_id"])) \
    #     .toDF(["id","shop_id"])
    # df3.printSchema()
    # df3.show()

    # for k in df2.collect():
    #     print(k.after)
    #     a = spark.read.json(k.after)
    #     print(a)


    # a = payload_df.map(lambda x: json.loads(x))\
    #     .map(lambda x: x['after'])\
    #     .foreach(lambda x: print(x))
    #     .selectExpr("CAST(after AS STRING) as after")\
    #     .alias("after")\
    #     .select(json_tuple(col("after"),"id","shop_id"))\
    #     .toDF("id",)\


    # value_test.printSchema()
    # value_test.show()
    # columns = ["id", "name","age","gender"]

    # # Create DataFrame 
    # data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    #     (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]
    # sampleDF = spark.sparkContext.parallelize(data).toDF(columns)
    # payload_df.show()

    # for k in payload_df.collect():
    #     a = k.selectExpr("CAST(after AS STRING) as value").alias("value")
    #     a.show()
        # spark.read.json(k.__getitem__('after'))
        # a = spark.createDataFrame([(k.__getitem__('after'))],["after"])
        # a.select(json_tuple(k("after"),"id","shop_id","app_id")) \
        #     .toDF("id","shop_id","app_id") \
        #     .show(truncate=False)
        # print(k.__getitem__('after'))
        # a=spark.createDataFrame([(1, jsonString)],["id","value"])
        # print(spark.read.json(k.__getitem__('after')))

    # sampleDF = payload_df.collect()
    # df = df.selectExpr("CAST(value AS STRING)")

    # df = df.select(json_tuple(col("value"),"payload")) \
    #     .toDF("payload")
    # df = df.select(json_tuple(col("payload"),"before","after","source","op")) \
    #     .toDF("before","after","source","op")
    # df.show()
    # print(sampleDF.after)
    # a = payload_df.withColumn("payload", to_json(col("payload")))
    # sampleDF.show(truncate=False)
    # payload_df.show(truncate=False)
    # json_df = df.selectExpr("CAST(value AS STRING) as value")
    # for item in payload_df.collect():
    #     print(item)
    # payload_df.show(truncate=False)

    # payload_df
    # spark.stop()


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
