from pyspark.sql import SparkSession

driver = "org.postgresql.Driver"
database_host = "fgtech-postgresql.cybhstjgn0kj.ap-southeast-1.rds.amazonaws.com"
database_port = "5432"
database_name = "psql_test"
table = "towns"
user = "fgtech"
password = "WcXuwUvsgGAyfrYP"
url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

def init_spark():
  spark = SparkSession.builder\
    .appName("trip-app")\
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

test()