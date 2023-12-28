from pyspark.sql import SparkSession

# Create spark session with hive enabled
spark = SparkSession.builder.appName("SparkByExamples.com").enableHiveSupport().getOrCreate()

columns = ["id", "name","age","gender"]

# Create DataFrame 
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]
sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

sampleDF.show()
# # Create Hive Internal table
# sampleDF.write.mode('overwrite') \
#          .saveAsTable("employee")

# # Read Hive table
# df = spark.read.table("employee")
# df.show()
