from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ShortType, FloatType
import json

spark = SparkSession.builder.appName("Hive Spark Connector App").enableHiveSupport().getOrCreate()

columns = ["title", "author","year","views"]

# Create DataFrame 
data = [(1, "James",30,1), (2, "Ann",40,1),
    (3, "Jeff",41,1),(4, "Jennifer",20,1)]
sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Create Hive Internal table
sampleDF.write.mode('overwrite') \
         .saveAsTable("books_ext")

# Read Hive table
df = spark.read.table("books_ext")
df.show()