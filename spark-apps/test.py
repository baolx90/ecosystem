from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Read HBase Table using PySpark Demo") \
    .config("spark.jars", "/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/hive/lib/hive-hbase-handler-2.1.1-cdh6.2.0.jar") \
    .config("spark.executor.extraClassPath", "/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/hive/lib/hive-hbase-handler-2.1.1-cdh6.2.0.jar") \
    .config("spark.executor.extraLibrary", "/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/hive/lib/hive-hbase-handler-2.1.1-cdh6.2.0.jar") \
    .config("spark.driver.extraClassPath", "/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/hive/lib/hive-hbase-handler-2.1.1-cdh6.2.0.jar") \
    .enableHiveSupport()\
    .getOrCreate()
print(spark.sparkContext.appName)
transaction_detail_df = spark.sql("use default")
transaction_detail_df = spark.sql("select * from emp")
transaction_detail_df.printSchema()
transaction_detail_df.show(2, False)