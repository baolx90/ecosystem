from pyspark.sql import SparkSession


driver = "com.mysql.cj.jdbc.Driver"
database_host = "ar6-beta-kafka-test.cybhstjgn0kj.ap-southeast-1.rds.amazonaws.com"
database_port = "3306"
database_name = "alireviews_sms"
table = "sms_settings"
user = "kafka"
password = "UVxRnwBHwGDCDDnR"
url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

def init_spark():
  spark = SparkSession.builder\
    .master("yarn")\
    .appName("yarn-app")\
    .getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def test():
  spark,sc = init_spark()
  df = spark.read\
    .format("jdbc")\
    .option("driver",driver)\
    .option("url", url)\
    .option("dbtable", table)\
    .option("user", user)\
    .option("password", password)\
    .load()

  df.show()

def main():
  test()

if __name__ == '__main__':
  main()