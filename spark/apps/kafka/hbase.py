from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ShortType, FloatType
import json
import happybase


# conf = new HBaseConfiguration()
# conf.set("hbase.zookeeper.quorum", "zoo1:2181")

# new HBaseContext(spark.sparkContext, conf)
def main():
    spark = SparkSession.builder.appName("HBase Spark Connector App").enableHiveSupport().getOrCreate()
    # df = spark.read.format('org.apache.hadoop.hbase.spark') \
    # .option('hbase.table','books') \
    # .option('hbase.columns.mapping', \
    #         'title STRING :key, \
    #         author STRING info:author, \
    #         year STRING info:year, \
    #         views STRING analytics:views') \
    # .option('hbase.use.hbase.context', False) \
    # .option('hbase.config.resources', 'file:///usr/hbase/conf/hbase-site.xml') \
    # .option('hbase-push.down.column.filter', False) \
    # .load()

    # df.show()

    connection = happybase.Connection('localhost')
    table = connection.table('books')

    table.put(b'title3', {b'info:author': b'value1',
                           b'info:year': b'1990'})

    row = table.row(b'title3')
    print(row[b'info:author'])  # prints 'value1'

    # for key, data in table.rows([b'row-key-1', b'row-key-2']):
    #     print(key, data)  # prints row key and data for each row

    # for key, data in table.scan(row_prefix=b'row'):
    #     print(key, data)  # prints 'value1' and 'value2'

    # row = table.delete(b'row-key')

    # spark = SparkSession.builder.appName("HBase Spark Connector App").enableHiveSupport().getOrCreate()

    # df = spark.read.format('org.apache.hadoop.hbase.spark') \
    # .option('hbase.table','books') \
    # .option('hbase.columns.mapping', \
    #         'title STRING :key, \
    #         author STRING info:author, \
    #         year STRING info:year, \
    #         views STRING analytics:views') \
    # .option('hbase.use.hbase.context', False) \
    # .option('hbase.config.resources', 'file:///usr/hbase/conf/hbase-site.xml') \
    # .option('hbase-push.down.column.filter', False) \
    # .load()

    # df.show()

    # new HBaseContext(spark.sparkContext, conf)

    # # transaction_detail_df = spark.sql("use default")
    # # transaction_detail_df = spark.sql("select * from hbase_hive_names")
    # # transaction_detail_df.printSchema()

    # # columns = ["hdid", "id", "fn","ln","age"]

    # # # Create DataFrame 
    # # data = [(1, 2, 3, 4, 5), (2, 3, 4, 5, 6)]
    # # sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

    # # sampleDF.show()
    # # sampleDF.write.mode("overwrite").saveAsTable("hbase_hive_names")

    # data = [(1, "Ranga", 34, 15000.5), (2, "Nishanth", 5, 35000.5),(3, "Meena", 30, 25000.5)]

    # schema = StructType([ \
    #     StructField("id",LongType(),True), \
    #     StructField("name",StringType(),True), \
    #     StructField("age",ShortType(),True), \
    #     StructField("salary", FloatType(), True)
    #   ])
 
    # employeeDF = spark.createDataFrame(data=data,schema=schema)

    # # catalog = """{
    # #     "table":{"namespace":"default", "name":"employees"},
    # #       "rowkey":"key",
    # #       "columns":{
    # #           "id":{"cf":"rowkey", "col":"key", "type":"long"},
    # #           "name":{"cf":"per", "col":"name", "type":"string"},
    # #           "age":{"cf":"per", "col":"age", "type":"short"},
    # #           "salary":{"cf":"prof", "col":"salary", "type":"float"}
    # #       }
    # # }"""

    # employeeDF.write.format("org.apache.hadoop.hbase.spark")\
    #     .option("hbase.columns.mapping","id STRING :key, name STRING c:name, age STRING c:age, salary STRING c:salary ")\
    #     .option("hbase.table", "employees")\
    #     .option("hbase.spark.use.hbasecontext", False)\
    #     .save()

    # df = spark.read.format("org.apache.hadoop.hbase.spark")\
    #     .option("hbase.columns.mapping","id INT :key, name STRING cf:name")\
    #     .option("hbase.table", "testspark")\
    #     .option("hbase.spark.use.hbasecontext", False)\
    #     .load()
    # df.show()
    # df = spark.read.format("org.apache.hadoop.hbase.spark").options(catalog=catalog).option("hbase.spark.use.hbasecontext", False).load()
    # df.show()

    # spark.stop()

if __name__ == "__main__":
    main()